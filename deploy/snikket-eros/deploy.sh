#!/usr/bin/env bash
# 在部署机上切换 eros-engine 到指定镜像并重启。
#
#   sudo ./deploy.sh 59a65db
#
# 镜像需要事先 docker load 进来（本机构建后 docker save | scp | docker load）。
set -euo pipefail

TAG="${1:-}"
DEPLOY_DIR="/etc/snikket-eros"

usage() { echo "用法: sudo $0 <image-tag>   例: sudo $0 59a65db" >&2; exit 2; }
[[ -n $TAG ]] || usage
[[ $EUID -eq 0 ]] || { echo "请用 root 运行" >&2; exit 1; }

docker image inspect "c1921/eros-engine:$TAG" >/dev/null 2>&1 \
    || { echo "镜像不存在: c1921/eros-engine:$TAG（先 docker load）" >&2; exit 1; }

TS=$(date -u +%Y%m%dT%H%M%SZ)
cp -a "$DEPLOY_DIR/compose.yml" "$DEPLOY_DIR/compose.yml.pre-$TAG-$TS"

sed -i "s|image: c1921/eros-engine:.*|image: c1921/eros-engine:$TAG|" "$DEPLOY_DIR/compose.yml"
grep -n 'image: c1921/eros-engine' "$DEPLOY_DIR/compose.yml"

cd "$DEPLOY_DIR"
docker compose up -d --no-deps engine
sleep 5
docker ps --filter name=eros-engine --format '{{.Names}} {{.Image}} {{.Status}}'
docker logs --since 30s snikket-eros-engine-1 2>&1 | tail -5

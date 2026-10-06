#!/usr/bin/env bash
# 本地验证入口：带竞态检测跑全部单测，-v 输出业务分组与每个用例的通过情况。
set -euo pipefail
cd "$(dirname "$0")/.."

export CGO_ENABLED=1

echo "== go vet =="
go vet ./...

echo "== go test -race -v =="
# 测试用中文业务名命名顶层测试/子测试（TestIngest/TestBars/TestRestart/
# TestSubscription/TestConcurrent），-v 日志可直接看到：
#   业务 -> 用例 -> PASS/FAIL
go test -race -v -count=1 ./...

package infra

import "time"

// startupPingTimeout 为外部基础设施启动期连通性探测超时时间。
const startupPingTimeout = 5 * time.Second

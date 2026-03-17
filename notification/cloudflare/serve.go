package cloudflare

import (
	"github.com/firetiger-oss/storage/notification"
)

func init() {
	notification.DefaultServeOptions = append(notification.DefaultServeOptions,
		notification.WithHandler("POST /cloudflare", NewQueuesHandler),
		notification.WithHandler("POST /cloudflare/batch", NewBatchQueuesHandler),
	)
}

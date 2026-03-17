package aws

import (
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/firetiger-oss/storage/notification"
)

func init() {
	// Register Lambda starter if running in Lambda environment
	// lambdacontext.FunctionName is populated by the Lambda runtime
	if lambdacontext.FunctionName != "" {
		notification.DefaultServeOptions = append(notification.DefaultServeOptions,
			notification.WithServe(func(h notification.ObjectHandler) {
				handler := NewS3LambdaHandler(h)
				lambda.Start(handler.HandleEvent)
			}),
		)
	}

	// Register EventBridge HTTP handler
	notification.DefaultServeOptions = append(notification.DefaultServeOptions,
		notification.WithHandler("POST /aws", NewS3EventBridgeHandler),
	)
}

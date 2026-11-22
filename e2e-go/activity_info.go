package main

import (
	"context"

	"go.temporal.io/sdk/activity"
)

func activityInfo(ctx context.Context) int32 { return activity.GetInfo(ctx).Attempt }

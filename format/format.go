package format

import (
	"github.com/H0RlZ0N/joy4/av/avutil"
	"github.com/H0RlZ0N/joy4/format/aac"
	"github.com/H0RlZ0N/joy4/format/flv"
	"github.com/H0RlZ0N/joy4/format/mp4"
	"github.com/H0RlZ0N/joy4/format/rtmp"
	"github.com/H0RlZ0N/joy4/format/rtsp"
	"github.com/H0RlZ0N/joy4/format/ts"
)

func RegisterAll() {
	avutil.DefaultHandlers.Add(mp4.Handler)
	avutil.DefaultHandlers.Add(ts.Handler)
	avutil.DefaultHandlers.Add(rtmp.Handler)
	avutil.DefaultHandlers.Add(rtsp.Handler)
	avutil.DefaultHandlers.Add(flv.Handler)
	avutil.DefaultHandlers.Add(aac.Handler)
}

package rtmp

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/H0RlZ0N/joy4/av"
	"github.com/H0RlZ0N/joy4/format/rawmedia"
)

type RtmpClient struct {
	ctx        context.Context
	cancelfunc context.CancelFunc

	conn     *Conn
	demuxer  *rawmedia.Demuxer
	mediaDir rawmedia.MediaDir

	pushSign   bool
	initHeader bool

	onEvent func(eCode EventCode)
}

type EventCode int32

const (
	RTMP_EVENT_CLOSED EventCode = -1
)

func NewRtmpClient() *RtmpClient {
	return &RtmpClient{}
}

func (rc *RtmpClient) Init(url string, streams []*rawmedia.Stream, OnEvent func(eCode EventCode)) error {
	var err error
	rc.ctx, rc.cancelfunc = context.WithCancel(context.Background())
	rc.conn, err = Dial(url)
	if err != nil {
		return err
	}
	rc.onEvent = OnEvent
	rc.demuxer = rawmedia.NewDemuxer()
	err = rc.demuxer.AddStreams(streams)
	if err != nil {
		return err
	}
	rc.pushSign = false

	return nil
}

func (rc *RtmpClient) Close() {
	log.Printf("-----------> conn close\n")
	if rc.cancelfunc != nil {
		rc.cancelfunc()
		rc.cancelfunc = nil
	}
	rc.pushSign = false
	if rc.conn != nil {
		rc.conn.Close()
		rc.conn = nil
	}
	rc.onEvent = nil
}

func (rc *RtmpClient) SetMediaDir(mediaDir rawmedia.MediaDir) {
	rc.mediaDir = mediaDir
}

func (rc *RtmpClient) WritePacket(packet rawmedia.Packet) error {
	_, err := rc.demuxer.WriteMediaData(packet)
	if err != nil {
		return err
	}
	return nil
}

// start rtmp publish
func (rc *RtmpClient) StartPublish() {
	if rc.conn != nil {
		rc.pushSign = true
	}
}

// stop rtmp publish
func (rc *RtmpClient) StopPublish() {
	rc.pushSign = false
}

func (rc *RtmpClient) Serve() {
	var err error
	tick := time.NewTicker(time.Second * 60)
	defer func() {
		log.Printf("defer close rtmp client\n")
		if rc.onEvent != nil {
			rc.onEvent(RTMP_EVENT_CLOSED)
		}
		if tick != nil {
			tick.Stop()
			tick = nil
		}
	}()

	t := time.NewTimer(5 * time.Millisecond)
	for {
		select {
		case <-rc.ctx.Done():
			return
		case <-tick.C:
			// 超过60s没有读出数据包，关闭连接
			tick.Stop()
			log.Printf("ReadPacket timeout\n")
			return
		case <-t.C:
			var pkt av.Packet
			pkt, err = rc.demuxer.ReadPacket()
			if err != nil {
			} else {
				tick.Reset(time.Second * 60)
				if err = rc.Sendpacket(pkt); err != nil {
					log.Printf("Sendpacket error %v\n", err.Error())
					return
				}
			}
			t.Reset(5 * time.Millisecond)
		}
	}
}

func (rc *RtmpClient) Sendpacket(pkt av.Packet) (err error) {
	if !rc.pushSign {
		return nil
	}

	if rc.conn == nil {
		return fmt.Errorf("rtmp disconnect")
	}

	// 视频sps，pps或音频adts未填充，等待填充后再开始传输
	streams := rc.demuxer.GetStreams()
	for _, stream := range streams {
		if rc.mediaDir&rawmedia.MEDIADIR_VIDEO != 0 {
			if stream.StreamType == rawmedia.StreamTypeH264 || stream.StreamType == rawmedia.StreamTypeH265 {
				if stream.CodecData == nil {
					return nil
				}
			}
		} else if rc.mediaDir&rawmedia.MEDIADIR_AUDIOSEND != 0 && stream.StreamType == rawmedia.StreamTypeAdtsAAC {
			if stream.CodecData == nil {
				return nil
			}
		}
	}

	if !rc.initHeader {
		log.Printf("WriteStreamsHeader\n")
		if err = rc.conn.WriteStreamsHeader(streams); err != nil {
			log.Printf("rtmp WriteStreamsHeader err: %v\n", err)
			return
		}
		rc.initHeader = true
	}

	if err = rc.conn.WritePacket(pkt); err != nil {
		log.Printf("rtmp WritePacket err: %v\n", err)
		rc.pushSign = false
		return
	}
	return nil
}

func GetSupportCodec() []rawmedia.DataType {
	var DataTypeArray []rawmedia.DataType
	DataTypeArray = append(DataTypeArray, rawmedia.StreamTypeH264)
	DataTypeArray = append(DataTypeArray, rawmedia.StreamTypeH265)
	DataTypeArray = append(DataTypeArray, rawmedia.StreamTypeAdtsAAC)
	return DataTypeArray
}

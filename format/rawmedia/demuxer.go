package rawmedia

import (
	"fmt"
	"os"
	"time"

	"github.com/nareix/joy4/av"
	"github.com/nareix/joy4/codec/aacparser"
	"github.com/nareix/joy4/codec/h264parser"
	"github.com/nareix/joy4/utils/bits/pio"
)

type Stream struct {
	av.CodecData
	demuxer    *Demuxer
	streamType uint8
	idx        int
}

type Demuxer struct {
	pkts    []av.Packet
	streams []*Stream
	stage   int
}

const (
	StreamTypeH264 = iota
	StreamTypeAdtsAAC
)

func NewDemuxer() *Demuxer {
	return &Demuxer{}
}

func (self *Demuxer) Streams() (streams []av.CodecData, err error) {

	streams = make([]av.CodecData, 2)
	for i, stream := range self.streams {
		streams[i] = stream.CodecData
	}

	if len(streams) == 0 {
		err = fmt.Errorf("no stream found")
	}

	return
}

func (self *Demuxer) AddVideoStreams() {
	stream := &Stream{}
	stream.idx = 0
	stream.demuxer = self
	stream.streamType = StreamTypeH264
	self.streams = append(self.streams, stream)
	return
}

func (self *Demuxer) AddAudioStreams() {
	stream := &Stream{}
	stream.idx = 1
	stream.demuxer = self
	stream.streamType = StreamTypeAdtsAAC
	self.streams = append(self.streams, stream)
	return
}

func (self *Demuxer) ReadPacket() (pkt av.Packet, err error) {

	if len(self.pkts) == 0 {
		return pkt, fmt.Errorf("EOF packet")
	}
	pkt = self.pkts[0]
	self.pkts = self.pkts[1:]
	return
}

var timeStamp time.Duration = 0

func (self *Stream) addPacket(payload []byte, iskeyframe bool, pts uint64) {
	demuxer := self.demuxer
	pkt := av.Packet{
		Idx:        int8(self.idx),
		IsKeyFrame: iskeyframe,
		Time:       time.Duration(pts) * time.Microsecond,
		Data:       payload,
	}
	demuxer.pkts = append(demuxer.pkts, pkt)
}

var h264fl *os.File = nil

func (self *Stream) PackMediaData(payload []byte, pts uint64) (n int, err error) {
	switch self.streamType {
	case StreamTypeAdtsAAC:
		var config aacparser.MPEG4AudioConfig
		for len(payload) > 0 {
			var hdrlen, framelen int
			if config, hdrlen, framelen, _, err = aacparser.ParseADTSHeader(payload); err != nil {
				return
			}
			if self.CodecData == nil {
				if self.CodecData, err = aacparser.NewCodecDataFromMPEG4AudioConfig(config); err != nil {
					return
				}
			}
			self.addPacket(payload[hdrlen:framelen], true, pts)
			n++
			payload = payload[framelen:]
		}

	case StreamTypeH264:
		nalus, _ := h264parser.SplitNALUs(payload)
		var sps, pps []byte
		for _, nalu := range nalus {
			if len(nalu) > 0 {
				var iskeyframe bool = false
				naltype := nalu[0] & 0x1f
				if naltype == 7 || naltype == 8 || naltype == 5 {
					iskeyframe = true
				}
				switch {
				case naltype == 7:
					sps = nalu
				case naltype == 8:
					pps = nalu
				case h264parser.IsDataNALU(nalu):
					// raw nalu to avcc
					b := make([]byte, 4+len(nalu))
					pio.PutU32BE(b[0:4], uint32(len(nalu)))
					copy(b[4:], nalu)
					self.addPacket(b, iskeyframe, pts)
					n++
				}
			}
		}
		if self.CodecData == nil && len(sps) > 0 && len(pps) > 0 {
			if self.CodecData, err = h264parser.NewCodecDataFromSPSAndPPS(sps, pps); err != nil {
				return
			}
		}
	}

	return
}

func (self *Demuxer) WriteMediaData(streamType uint8, data []byte, pts uint64) (n int, err error) {
	for _, stream := range self.streams {
		var i int
		if stream.streamType != streamType {
			continue
		}
		if i, err = stream.PackMediaData(data, pts); err != nil {
			return
		}
		n += i
	}
	return
}

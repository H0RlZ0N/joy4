package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/nareix/joy4/av"
	"github.com/nareix/joy4/av/avutil"
	"github.com/nareix/joy4/av/pktque"
	"github.com/nareix/joy4/codec/aacparser"
	"github.com/nareix/joy4/format"
	"github.com/nareix/joy4/format/rawmedia"
	"github.com/nareix/joy4/format/rtmp"
)

func init() {
	format.RegisterAll()
}

func RtmpPushData(conn *rtmp.Conn, src av.Demuxer) (err error) {
	streams, err := src.Streams()
	if err != nil {
		fmt.Printf("Get Streams err: %v\n", err)
		return
	}
	fmt.Printf("Streams ok: %v\n", len(streams))
	if err = conn.WriteHeader(streams); err != nil {
		return
	}

	for {
		var pkt av.Packet
		pkt, err = src.ReadPacket()
		if pkt.Idx == 00 {
			time.Sleep(time.Millisecond * 10)
		} else {
			time.Sleep(time.Millisecond * 1)
		}
		if err != nil {
			fmt.Printf("ReadPacket err: %v\n", err)
		} else {
			if err = conn.WritePacket(pkt); err != nil {
				fmt.Printf("rtmp WritePacket err: %v\n", err)
				conn.Close()
				return
			}
		}

	}
}

var startCode []byte = []byte{0x00, 0x00, 0x00, 0x01}

func PushH264() {
	demuxer := rawmedia.NewDemuxer()

	// h264
	data, err := ioutil.ReadFile("test.h264")
	demuxer.AddVideoStreams()
	if err != nil {
		panic(err)
	}
	bcheck := true
	pre := 0
	index := 0
	var pts uint64 = 0
	go func() {
		for {
			if bytes.Compare(data[index:index+4], startCode) == 0 {
				if index > pre {
					if bcheck {
						preNaluType := data[pre+4] & 0x1F
						naluType := data[index+4] & 0x1F
						if preNaluType == 7 {
							if naluType != 7 && naluType != 8 {
								bcheck = false
							}
							index += 4
							continue
						}
					}

					bcheck = true
					demuxer.WriteMediaData(rawmedia.StreamTypeH264, data[pre:index], pts)
					pts += 30000
					time.Sleep(time.Millisecond * 10)

					pre = index
					index += 4
					continue
				}
			}
			index++
			if index+4 >= len(data) {
				break
			}
		}
	}()

	// aac
	apre := 0
	aindex := 0
	audiodata, err := ioutil.ReadFile("test.aac")
	demuxer.AddAudioStreams()
	go func() {
		var audiopts uint64 = 0
		for {
			var _, framelen int
			if _, _, framelen, _, err = aacparser.ParseADTSHeader(audiodata[apre : apre+7]); err != nil {
				fmt.Printf("ParseADTSHeader err: %v\n", err)
				return
			}
			aindex = apre + framelen
			demuxer.WriteMediaData(rawmedia.StreamTypeAdtsAAC, audiodata[apre:aindex], audiopts)
			audiopts += 30000
			time.Sleep(time.Millisecond * 1)
			apre = aindex
			if apre+7 >= len(audiodata) {
				break
			}
		}
	}()

	time.Sleep(time.Millisecond * 2000)

	conn, err := rtmp.Dial("rtmp://127.0.0.1:1935/live/test")
	if err != nil {
		fmt.Printf("Dial err: %v\n", err)
		return
	}

	// send data
	RtmpPushData(conn, demuxer)

}

// as same as: ffmpeg -re -i projectindex.flv -c copy -f flv rtmp://localhost:1936/app/publish

func ts2rtmp() {
	file, _ := avutil.Open("out.ts")
	conn, _ := rtmp.Dial("rtmp://127.0.0.1:1935/live/test")

	demuxer := &pktque.FilterDemuxer{Demuxer: file, Filter: &pktque.Walltime{}}
	err := avutil.CopyFile(conn, demuxer)
	fmt.Printf("CopyFile error: %v\n", err)

	file.Close()
	conn.Close()
}

func flv2rtmp() {
	file, _ := avutil.Open("projectindex.flv")
	conn, _ := rtmp.Dial("rtmp://127.0.0.1:1935/live/test")
	// conn, _ := avutil.Create("rtmp://localhost:1936/app/publish")

	demuxer := &pktque.FilterDemuxer{Demuxer: file, Filter: &pktque.Walltime{}}
	err := avutil.CopyFile(conn, demuxer)
	fmt.Printf("CopyFile error: %v\n", err)

	file.Close()
	conn.Close()
}

func main() {
	PushH264()
	//ts2rtmp()
}

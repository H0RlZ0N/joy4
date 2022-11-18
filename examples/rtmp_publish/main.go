package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/H0RlZ0N/joy4/av"
	"github.com/H0RlZ0N/joy4/av/avutil"
	"github.com/H0RlZ0N/joy4/av/pktque"
	"github.com/H0RlZ0N/joy4/codec/aacparser"
	"github.com/H0RlZ0N/joy4/format"
	"github.com/H0RlZ0N/joy4/format/rawmedia"
	"github.com/H0RlZ0N/joy4/format/rtmp"
)

func init() {
	format.RegisterAll()
}

func RtmpPushData(conn *rtmp.Conn, src *rawmedia.Demuxer) (err error) {
	if err = conn.WriteStreamsHeader(src.GetStreams()); err != nil {
		return
	}

	for {
		var pkt av.Packet
		pkt, err = src.ReadPacket()
		fmt.Printf("Idx %v\n", pkt.Idx)
		if pkt.Idx == 00 {
			time.Sleep(time.Millisecond * 10)
		} else {
			time.Sleep(time.Millisecond * 10)
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
	demuxer.FillStreams()

	// h264
	data, err := ioutil.ReadFile("test.h264")
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
					var packet rawmedia.Packet
					packet.Datatype = rawmedia.StreamTypeH264
					packet.Data = data[pre:index]
					packet.Pts = pts
					demuxer.WriteMediaData(packet)
					pts += 100000
					time.Sleep(time.Millisecond * 100)

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
	go func() {
		var audiopts uint64 = 0
		for {
			var _, framelen int
			if _, _, framelen, _, err = aacparser.ParseADTSHeader(audiodata[apre : apre+7]); err != nil {
				fmt.Printf("ParseADTSHeader err: %v\n", err)
				return
			}
			aindex = apre + framelen
			var packet rawmedia.Packet
			packet.Datatype = rawmedia.StreamTypeAdtsAAC
			packet.Data = audiodata[apre : apre+framelen]
			packet.Pts = audiopts
			demuxer.WriteMediaData(packet)
			audiopts += 10000
			time.Sleep(time.Millisecond * 10)
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

// ffplay rtmp://127.0.0.1:1935/live/test
func rtmpclienttest() {
	rtmpClient := rtmp.NewRtmpClient()
	rtmpClient.Init("rtmp://127.0.0.1:1935/live/test")

	// h264
	data, err := ioutil.ReadFile("test.h264")
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
					var packet rawmedia.Packet
					packet.Datatype = rawmedia.StreamTypeH264
					packet.Data = data[pre:index]
					packet.Pts = pts
					rtmpClient.WritePacket(packet)
					pts += 60000
					time.Sleep(time.Millisecond * 30)

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
	go func() {
		var audiopts uint64 = 0
		for {
			var _, framelen int
			if _, _, framelen, _, err = aacparser.ParseADTSHeader(audiodata[apre : apre+7]); err != nil {
				fmt.Printf("ParseADTSHeader err: %v\n", err)
				return
			}
			aindex = apre + framelen
			var packet rawmedia.Packet
			packet.Datatype = rawmedia.StreamTypeAdtsAAC
			packet.Data = audiodata[apre : apre+framelen]
			packet.Pts = audiopts
			rtmpClient.WritePacket(packet)
			audiopts += 20000
			time.Sleep(time.Millisecond * 10)
			apre = aindex
			if apre+7 >= len(audiodata) {
				break
			}
		}
	}()

	go func() {
		rtmpClient.Serve()
	}()

	time.Sleep(time.Millisecond * 2000)
	rtmpClient.StartPublish()

	time.Sleep(time.Second * 660)

	log.Printf("main close rtmp client\n")
	rtmpClient.StopPublish()
	rtmpClient.Close()
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
	//PushH264()
	//ts2rtmp()
	rtmpclienttest()
}

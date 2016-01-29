/*
 * Copyright (c) 2013 IBM Corp.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *    Seth Hoenig
 *    Allan Stockdill-Mander
 *    Mike Robertson
 */

package mqtt

import (
	"errors"
	"fmt"

	"github.com/antlinker/mqtt-cli/packets"

	"sync"
	"time"
)

type lastcontact struct {
	sync.Mutex
	lasttime time.Time
}

func (l *lastcontact) update() {
	l.Lock()
	defer l.Unlock()
	l.lasttime = time.Now()

}

func (l *lastcontact) get() time.Time {
	l.Lock()
	defer l.Unlock()
	return l.lasttime
}

func keepalive(c *Client) {
	var err error
	defer func(c *Client) {
		c.workers.Done()

		if err != nil {
			ERROR.Println(NET, c.options.ClientID+":keepalive closed :", err)
			c.internalConnLost(err)
			return
		}
		if e := recover(); e != nil {
			ERROR.Println(NET, c.options.ClientID, ":", "keepalive panic closed :", e)
			c.internalConnLost(fmt.Errorf("读取数据出现严重错误:%v", e))
			return
		}
		ERROR.Println(NET, c.options.ClientID+":keepalive closed ")
	}(c)
	DEBUG.Println(PNG, "keepalive starting")
	c.pingOutstanding = false
	waittime := c.options.KeepAlive
	for {
		// keeptime := time.Now().Add(c.options.KeepAlive)
		// keep := uint(time.Since(c.lastContact.get()).Seconds())
		// if keep > c.options.KeepAlive.Seconds() {
		// 	keeptime = time.Now().Add(c.options.KeepAlive)
		// } else {
		// 	time.Now().Add(c.options.KeepAlive)
		// }
		select {
		case <-c.stop:
			DEBUG.Println(PNG, "keepalive stopped")
			//c.workers.Done()
			return
		case <-time.After(waittime):
			last := uint(time.Since(c.lastContact.get()).Seconds())
			//DEBUG.Printf("%s last contact: %d (timeout: %d) %b", PNG, last, uint(c.options.KeepAlive.Seconds()), c.pingOutstanding)
			if last >= uint(c.options.KeepAlive.Seconds()) {
				if !c.pingOutstanding {
					//DEBUG.Println(NET, "keepalive sending ping")
					ping := packets.NewControlPacket(packets.Pingreq).(*packets.PingreqPacket)
					c.pingOutstanding = true
					//We don't want to wait behind large messages being sent, the Write call
					//will block until it it able to send the packet.
					//ping.Write(c.conn)
					sendServer(c, ping, nil)
					waittime = c.options.KeepAlive / 4
					if waittime == 0 {
						waittime = 1 * time.Second
					}
					continue
				} else {
					//CRITICAL.Println(PNG, "pingresp not received, disconnecting")
					err = errors.New("pingresp not received, disconnecting")
					return
				}
			}
			waittime = c.options.KeepAlive - time.Since(c.lastContact.get())

		}
	}
}

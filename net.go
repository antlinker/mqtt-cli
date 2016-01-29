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
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"reflect"
	"time"

	"github.com/antlinker/mqtt-cli/packets"
	"golang.org/x/net/websocket"
)

func openConnection(uri *url.URL, tlsc *tls.Config, timeout time.Duration) (net.Conn, error) {
	switch uri.Scheme {
	case "ws":
		conn, err := websocket.Dial(uri.String(), "mqtt", "ws://localhost")
		if err != nil {
			return nil, err
		}
		conn.PayloadType = websocket.BinaryFrame
		return conn, err
	case "wss":
		config, _ := websocket.NewConfig(uri.String(), "ws://localhost")
		config.Protocol = []string{"mqtt"}
		config.TlsConfig = tlsc
		conn, err := websocket.DialConfig(config)
		if err != nil {
			return nil, err
		}
		conn.PayloadType = websocket.BinaryFrame
		return conn, err
	case "tcp":
		conn, err := net.DialTimeout("tcp", uri.Host, timeout)
		if err != nil {
			return nil, err
		}
		return conn, nil
	case "ssl":
		fallthrough
	case "tls":
		fallthrough
	case "tcps":
		conn, err := tls.DialWithDialer(&net.Dialer{Timeout: timeout}, "tcp", uri.Host, tlsc)
		if err != nil {
			return nil, err
		}
		return conn, nil
	}
	return nil, errors.New("Unknown protocol")
}

// actually read incoming messages off the wire
// send Message object into ibound channel
func incoming(c *Client) {

	var err error
	defer func(c *Client) {

		c.workers.Done()
		if err != nil {
			ERROR.Println(NET, c.options.ClientID, ":", "incoming closed:", err)
			c.internalConnLost(err)
			return
		}
		if e := recover(); e != nil {
			ERROR.Println(NET, c.options.ClientID, ":", "incoming panic closed :", e)
			c.internalConnLost(fmt.Errorf("读取数据出现严重错误:%v", e))
			return
		}
		ERROR.Println(NET, c.options.ClientID, ":", "incoming closing")

	}(c)

	var cp packets.ControlPacket

	DEBUG.Println(NET, "incoming started")

	for {
		if cp, err = packets.ReadPacket(c.conn); err != nil {
			return
		}
		DEBUG.Println(NET, "Received Message")
		c.lastContact.update()
		c.pingOutstanding = false
		select {
		case c.ibound <- cp:

		case <-c.stop:
			DEBUG.Println(NET, "incoming stopped")
			return
		}

	}
}

// receive a Message object on obound, and then
// actually send outgoing message to the wire
func outgoing(c *Client) {
	var err error
	defer func(c *Client) {

		c.workers.Done()
		if err != nil {
			ERROR.Println(NET, c.options.ClientID, ":", "outgoing closing:", err)
			c.internalConnLost(err)
			return
		}
		if e := recover(); e != nil {
			ERROR.Println(NET, c.options.ClientID, ":", "outgoing panic closed :", e)
			c.internalConnLost(fmt.Errorf("读取数据出现严重错误:%v", e))
			return
		}
		ERROR.Println(NET, c.options.ClientID, ":", "outgoing closed ")

	}(c)
	DEBUG.Println(NET, "outgoing started")

	for {
		DEBUG.Println(NET, "outgoing waiting for an outbound message")
		select {
		case <-c.stop:
			DEBUG.Println(NET, "outgoing stopped")
			return
		case pub := <-c.obound:
			msg := pub.p.(*packets.PublishPacket)
			if msg.Qos != 0 && msg.MessageID == 0 {
				msg.MessageID = c.getID(pub.t)
				pub.t.(*PublishToken).messageID = msg.MessageID
				persistOutbound(c.persist, msg)
			}

			if c.options.WriteTimeout > 0 {
				c.conn.SetWriteDeadline(time.Now().Add(c.options.WriteTimeout))
			}

			if err = msg.Write(c.conn); err != nil {
				ERROR.Println(NET, c.options.ClientID+":outgoing stopped with error 1")
				//c.errors <- err
				return
			}

			if c.options.WriteTimeout > 0 {
				// If we successfully wrote, we don't want the timeout to happen during an idle period
				// so we reset it to infinite.
				c.conn.SetWriteDeadline(time.Time{})
			}

			if msg.Qos == 0 {
				pub.t.flowComplete()
			}

			c.lastContact.update()
			DEBUG.Println(NET, "obound wrote msg, id:", msg.MessageID)
		case msg := <-c.oboundP:

			switch msg.p.(type) {
			case *packets.SubscribePacket:
				msg.p.(*packets.SubscribePacket).MessageID = c.getID(msg.t)
			case *packets.UnsubscribePacket:
				msg.p.(*packets.UnsubscribePacket).MessageID = c.getID(msg.t)
			}
			persistOutbound(c.persist, msg.p)
			DEBUG.Println(NET, "obound priority msg to write, type", reflect.TypeOf(msg.p))
			if err = msg.p.Write(c.conn); err != nil {
				ERROR.Println(NET, c.options.ClientID+":outgoing stopped with error 2")
				//c.errors <- err
				return
			}
			c.lastContact.update()
			switch msg.p.(type) {
			case *packets.DisconnectPacket:
				msg.t.(*DisconnectToken).flowComplete()
				DEBUG.Println(NET, "outbound wrote disconnect, stopping")
				return
			}
		}
	}
}

// receive Message objects on ibound
// store messages if necessary
// send replies on obound
// delete messages from store if necessary
func alllogic(c *Client) {
	var err error
	defer func(c *Client) {
		c.workers.Done()

		if err != nil {
			ERROR.Println(NET, c.options.ClientID+":alllogic stopped :", err)
			c.internalConnLost(err)
			return
		}
		if e := recover(); e != nil {
			ERROR.Println(NET, c.options.ClientID, ":", "alllogic panic closed :", e)
			c.internalConnLost(fmt.Errorf("处理数据逻辑出现严重错误:%v", e))
			return
		}
		ERROR.Println(NET, c.options.ClientID+":alllogic closed ")
	}(c)
	DEBUG.Println(NET, "logic started")

	for {
		DEBUG.Println(NET, "logic waiting for msg on ibound")

		select {
		case msg := <-c.ibound:
			DEBUG.Println(NET, "logic got msg on ibound, type", reflect.TypeOf(msg))
			persistInbound(c.persist, msg)

			switch msg.(type) {
			case *packets.PingrespPacket:
				DEBUG.Println(NET, "received pingresp")
				c.pingOutstanding = false
			case *packets.SubackPacket:
				sa := msg.(*packets.SubackPacket)
				DEBUG.Println(NET, "received suback, id:", sa.MessageID)
				token := c.getToken(sa.MessageID).(*SubscribeToken)
				DEBUG.Println(NET, "granted qoss", sa.GrantedQoss)
				for i, qos := range sa.GrantedQoss {
					token.subResult[token.subs[i]] = qos
				}
				token.flowComplete()
				go c.freeID(sa.MessageID)
			case *packets.UnsubackPacket:
				ua := msg.(*packets.UnsubackPacket)
				DEBUG.Println(NET, "received unsuback, id:", ua.MessageID)
				token := c.getToken(ua.MessageID).(*UnsubscribeToken)
				token.flowComplete()
				go c.freeID(ua.MessageID)
			case *packets.PublishPacket:
				pp := msg.(*packets.PublishPacket)
				DEBUG.Println(NET, "received publish, msgId:", pp.MessageID)
				DEBUG.Println(NET, "putting msg on onPubChan")
				DEBUG.Println(NET, "done putting msg on incomingPubChan")
				select {
				case c.incomingPubChan <- pp:
				case <-c.stop:
					return
				}
				switch pp.Qos {
				case 2:
					pr := packets.NewControlPacket(packets.Pubrec).(*packets.PubrecPacket)
					pr.MessageID = pp.MessageID

					DEBUG.Println(NET, "putting pubrec msg on obound")
					if !sendServer(c, pr, nil) {
						return
					}
					DEBUG.Println(NET, "done putting pubrec msg on obound")
				case 1:
					pa := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
					pa.MessageID = pp.MessageID
					DEBUG.Println(NET, "putting puback msg on obound")
					if !sendServer(c, pa, nil) {
						return
					}
					DEBUG.Println(NET, "done putting puback msg on obound")
				}
			case *packets.PubackPacket:
				pa := msg.(*packets.PubackPacket)
				DEBUG.Println(NET, "received puback, id:", pa.MessageID)
				// c.receipts.get(msg.MsgId()) <- Receipt{}
				// c.receipts.end(msg.MsgId())
				c.getToken(pa.MessageID).flowComplete()
				c.freeID(pa.MessageID)
			case *packets.PubrecPacket:
				prec := msg.(*packets.PubrecPacket)
				DEBUG.Println(NET, "received pubrec, id:", prec.MessageID)
				prel := packets.NewControlPacket(packets.Pubrel).(*packets.PubrelPacket)
				prel.MessageID = prec.MessageID
				if !sendServer(c, prel, nil) {
					return
				}
			case *packets.PubrelPacket:
				pr := msg.(*packets.PubrelPacket)
				DEBUG.Println(NET, "received pubrel, id:", pr.MessageID)
				pc := packets.NewControlPacket(packets.Pubcomp).(*packets.PubcompPacket)
				pc.MessageID = pr.MessageID
				if !sendServer(c, pc, nil) {
					return
				}

			case *packets.PubcompPacket:
				pc := msg.(*packets.PubcompPacket)
				DEBUG.Println(NET, "received pubcomp, id:", pc.MessageID)
				c.getToken(pc.MessageID).flowComplete()
				c.freeID(pc.MessageID)
			}

		case <-c.stop:
			WARN.Println(NET, c.options.ClientID+":logic stopped")
			return

		}

	}
}

func sendServer(c *Client, cp packets.ControlPacket, token Token) bool {
	select {
	case c.oboundP <- &PacketAndToken{p: cp, t: token}:
		return true
	case <-c.stop:
		return false
	}
}

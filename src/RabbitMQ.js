const amqp = require('amqplib/callback_api');
const uuid = require('uuid');

module.exports = class RabbitMQ {
    constructor({ callback = null, address, q, durable = true }) {
        this.callback = callback;
        this.address = address;
        this.q = q;
        this.durable = durable;

        this.rpcRabbit = this.rpcRabbit.bind(this)
        this.publisher = this.publisher.bind(this)
        this.consumer = this.consumer.bind(this)
    }
    rpcRabbit() {
        amqp.connect(this.address, (error0, conn) => {
            if (error0) throw error0;
            conn.createChannel(async (error1, ch) => {
                if (error1) throw error1;
                console.log(`Escutando Fila ${this.q}`);
                ch.assertQueue(this.q, { durable: this.durable });
                ch.prefetch(1);
                let _self = this
                ch.consume(this.q, async function reply (msg) {
                    try {
                        let msgParsed = JSON.parse(msg.content.toString())
                        let res = await _self.callback(msgParsed);
                        ch.sendToQueue(msg.properties.replyTo,
                            new Buffer(JSON.stringify(res)),
                            { correlationId: msg.properties.correlationId });
                        ch.ack(msg);
                    } catch(e) {
                        console.error(e);
                        ch.sendToQueue(msg.properties.replyTo,
                            new Buffer(JSON.stringify({})),
                            { correlationId: msg.properties.correlationId });
                        ch.ack(msg);
                    }
                });
            });
        });
    }

    rpcPublisher(obj = {}) {
        return new Promise((resolve) => {
            amqp.connect(this.address, (error0, conn) => {
                if (error0) throw error0;
                conn.createChannel((err, ch) => {
                    ch.assertQueue('', { exclusive: true }, (err, q) => {
                        const corr = uuid();
                        console.log(`Enviando Para Fila ${this.q}`);
                        ch.sendToQueue(this.q,
                            new Buffer(JSON.stringify(obj)),
                            { correlationId: corr, replyTo: q.queue });
            
                        ch.consume(q.queue, (msg) => {
                            if (msg.properties.correlationId === corr) {
                                try {
                                    if (this.callback !== null) this.callback(JSON.parse(msg.content.toString()));

                                    resolve(JSON.parse(msg.content.toString()))
                                    setTimeout(() => { conn.close(); }, 500);

                                } catch(e) {
                                    console.log(e)
                                    if (this.callback !== null) this.callback();
                                    resolve()
                                }
                            }
                        }, { noAck: true });
                    });
                });
            });
        })
    }

    publisher(msg = {}) {
        amqp.connect(this.address, (error0, conn) => {
            if (error0) throw error0;
            console.log(`Enviando Para Fila ${this.q}`);
            conn.createChannel((err, ch) => {
                if (err != null) bail(err);
                try {
                    ch.assertQueue(this.q);
                    console.log(" [x] Sent %s", JSON.stringify(msg));
                    ch.sendToQueue(this.q, Buffer.from(JSON.stringify(msg)));
                } catch(e) {
                    console.error(e);
                }
            });
        });
    }

    consumer() {
        return new Promise((resolve) => {
            amqp.connect(this.address, (error0, conn) => {
                if (error0) throw error0;
                console.log(`Escutando Fila ${this.q}`);
                conn.createChannel(async (err, ch) => {
                    if (err != null) bail(err);
                    ch.assertQueue(this.q);
                    ch.consume(this.q, (msg) => {
                        if (msg !== null) {
                            try {
                                let msgParsed = JSON.parse(msg.content.toString())
                                if (this.callback !== null) this.callback(msgParsed);
    
                                resolve(msgParsed)
                                ch.ack(msg);
                            } catch (e) {
                                console.log(e)
                                if (this.callback !== null) this.callback();
    
                                resolve()
                                ch.ack(msg);
                            }
                        }
                    });
                });
            });
        })
    }
}
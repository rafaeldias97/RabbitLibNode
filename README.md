# Rabbit Lib Node

This library abstracts the amqplib layers and can work with a cleaner code.
### Exemplo Methods
* consumer
* publisher
* rpcRabbit
* rpcPublisher

## How to Install
> npm install --save rabbit-lib-node

## Usage

This session will show you how to use the library:

![diagram](./static/diagram.PNG)

##### How to import
```JS
const Rabbit = require('rabbit-lib-node');
```
##### Consumer Example
```JS
new Rabbit({ callback: (msg) => {
	console.log(msg)
}, address: 'amqp://guest:guest@localhost:5672',
q:  'queue.topic' }).consumer()
```

##### Publisher Example
```JS
new Rabbit({ 
	address: 'amqp://guest:guest@localhost:5672',
	q: 'queue.topic' }).publisher({...})
```

#####  RPC Consumer Example
```JS
new Rabbit({ callback: (msg) => {
	console.log(msg)
}, address: 'amqp://guest:guest@localhost:5672',
	q: 'queue.topic' }).rpcRabbit()
```

#####  RPC Publisher Example
```JS
new Rabbit({ callback: (msg) => {
	console.log(msg)
}, address: 'amqp://guest:guest@localhost:5672',
	q: 'queue.topic' }).rpcPublisher({...})
```
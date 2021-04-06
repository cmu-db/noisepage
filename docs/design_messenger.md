# Design Doc: Messenger

## Overview

- 2020-10-12 [design slides](https://drive.google.com/file/d/1RQSPKAsLvxcQ-hwpG8FOOVp8rFlVYXJN/).

The Messenger is designed to abstract around the transport protocol being used. Building off ZeroMQ, there is support
for network ports, IPC, and inproc communication. See the design slides for more details.

The Messenger was designed around the high-level API of  
```SendMessage(destination, message, callback_to_be_invoked_on_response)```  
In other words, every message that is sent is associated with a callback that should be invoked on the response. We call
these message-specific callbacks (MSCs).

Additionally, the Messenger was designed so that different system components could spin up their own custom "server
loop" that would handle their own message types.

For example, replication is one server loop,  
```messenger->ListenForConnection(port 15445, [](...){ replication_logic });```,    
and the model server manager used for machine learning stuff is another server loop,  
```messenger->ListenForConnection(port 15446, [](...){ model_server_manager_logic });```.

You can think of a server-loop as a persistent callback, permanently attached to the new connection endpoint.  
We refer to the custom server-loop functions as server-loop callbacks (SLCs). Be careful not to confuse server-loop
callbacks with message-specific callbacks.

### ZeroMQ quirks

#### Socket ownership

In ZeroMQ, a socket must be used from the same thread that **created** it.  
This is different from the usual C++ programming model, where multithreading would be fine with just a shared
mutex-protected queue.  
To maintain this ZeroMQ requirement, the `Messenger` is responsible for creating all of the sockets that are involved
with the sending and receiving of messages.  
This means that all `SendMessage` calls are actually just buffering messages to the `Messenger`.  
`SendMessage` however returns immediately since it is unreliable to use the sent state of a message in designing your
protocols; see below on waiting until messages are sent.

#### Delivery guarantees

**Note that ZeroMQ has exactly one guarantee: all-or-nothing message delivery.**  
ZeroMQ does **not** have guaranteed delivery, meaning that messages may randomly be dropped and it is up to the caller
to retry.  
To address this, all implementations of the Messenger protocol are expected to:

1. Maintain a list of pending messages, until the message gets acknowledged.
2. Periodically retry pending messages.
3. Fake idempotence: if the same message is received more than once, don't forward it to the SLC.

Faking idempotence requires tracking what messages have been seen so far. A reasonably efficient algorithm is presented
below.

### Message format

The message format is described in the slides, but essentially is equivalent to the following Python code:  
```"{}-{}-{}-{}".format(message_id, source_callback_id, dest_callback_id, message_contents)```

Because every sent message is associated with a callback ID, the message itself must contain **two** callback IDs:

- one for the recipient of the message to act on, the callback ID that they specified,
- one for the recipient of the message to send you back later, for your own callback that acts on the recipient's
  response.

If you are **sending** a message for the **first time**, i.e., you are starting the conversation, then you probably will
end up doing    
```SendMessage(..., static_cast<uint64_t>(messenger::Messenger::BuiltinCallback::NOOP))```

### Invoking callbacks

The messenger runs its own `ProcessMessage()` that invokes the MSC before invoking the custom SLC.  
This is a consequence of the current Messenger design, where the Messenger is the owner of all of the callbacks that are
created with every `SendMessage`.

### Tracking seen messages efficiently

As described above, ZeroMQ does not guarantee message delivery of any kind whatsoever.  
But if a message is already in-flight and the retry mechanism fires, the retry mechanism may send the same message more
than once.    
However, applying the same message more than once is clearly bad -- consider "UPDATE foo SET x = x + 1".  
So before applying a message, it is important to check that the specific message ID has not been seen before.

Naively, it is possible to track what messages have been seen by doing the following:

1. Maintain a `set<message_id_t> seen_` of all messages seen so far.
2. As each message is received, check against `seen_`.

- If the message is already in `seen_`, ignore it (at-most-once).
- If the message is not in `seen_`, process it (at-least-once).

However, the problem has the following characteristics that we can exploit:

- Message IDs are monotonically increasing.
- On average, we expect most message deliveries to succeed.
    - If you maintain `max_seen_message_id`, you expect a "small" number of messages to be unseen.

With much thanks to PK and FF, especially for PK's suggestion to consider the complement,

- Maintain the highest message ID seen so far, which is just an int.
- Maintain the complement (unseen messages so far), which is expected to be a small set.

```python
# Input:
#   current : int = The new ID that is currently being seen.
#   max_seen : int = Maximum message ID seen so far.
#   complement : set(int) = Unseen messages with IDs < max_seen.
# Output:
#   first_time : bool = True if message is seen for first time.
#   max_seen : int = Maximum message ID seen so far.
#   complement : set(int) = Unseen messages with IDs < max_seen.
def is_first_time(current, max_seen, complement):
    first_time = False
    if current > max_seen:
        for i in range(max_seen + 1, current):
            complement.add(i)
        max_seen = current
        first_time = True
    else:
        if current in complement:
            complement.remove(current)
            first_time = True
    return first_time, max_seen, complement
```

### On waiting until messages are sent

I currently believe that you do not want to even expose "yes, this message has been sent" data to a user of the
Messenger.  
Consider the following example:

1. Primary -> Replica: "here's a bunch of records".
2. At this point, the message has been sent. But it is not safe to assume that the replica didn't crash immediately
   afterwards.
3. Instead, the replica should explicitly acknowledge that the records have been received (after perhaps persisting
   them) and/or applied.

In general, explicitly communicate instead of relying on the sent/unsent status of a message.

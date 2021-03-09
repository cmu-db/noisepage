# Design Doc: Messenger

## Overview

- 2020-10-12 [design slides](https://drive.google.com/file/d/1RQSPKAsLvxcQ-hwpG8FOOVp8rFlVYXJN/).

The Messenger is designed to abstract around the transport protocol being used.
Building off ZeroMQ, there is support for network ports, IPC, and inproc communication.
See the design slides for more details.

The Messenger was designed around the high-level API of  
```SendMessage(destination, message, callback_to_be_invoked_on_response)```  
In other words, every message that is sent is associated with a callback that should be invoked on the response.
We call these message-specific callbacks (MSCs).

Additionally, the Messenger was designed so that different system components could spin up their own custom "server loop" that would handle their own message types.  

For example, replication is one server loop,  
```messenger->ListenForConnection(port 15445, [](...){ replication_logic });```,    
and the model server manager used for ML stuff is another server loop,  
```messenger->ListenForConnection(port 15446, [](...){ model_server_manager_logic });```.

You can think of a server-loop as a persistent callback, permanently attached to the new connection endpoint.  
We refer to the custom server-loop functions as server-loop callbacks (SLCs).
Be careful not to confuse server-loop callbacks with message-specific callbacks.

### Message format

The message format is described in the slides, but essentially is equivalent to the following Python code:  
```"{}-{}-{}".format(source_callback_id, dest_callback_id, message_contents)```

Because every sent message is associated with a callback ID, the message itself must contain **two** callback IDs:

- one for the recipient of the message to act on, the callback ID that they specified,
- one for the recipient of the message to send you back later, for your own callback that acts on the recipient's response.

If you are **sending** a message for the **first time**, i.e., you are starting the conversation, then you probably will end up doing    
```SendMessage(..., static_cast<uint64_t>(messenger::Messenger::BuiltinCallback::NOOP))```

This is a consequence of the current Messenger design, where the Messenger is the owner of all of the callbacks that are created with every `SendMessage`.  
The messenger runs its own `ProcessMessage()` that invokes the MSC before invoking the custom SLC.
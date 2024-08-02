<h1 align="center">SchedulerGodX <code>v1.0.0</code></h1>

## Navigation

- [Short description](#short-description)
- [Example usage](#exemple-usage)
- [Client](#client)
  - [Initialization](#initialization)
  - [Create task](#create-task)
  - [Task set parametrs](#task-set-parametrs)
  - [Task launch](#task-launch)
  - [More methods](#more-methods)
    - [Threads](#threads)
    - [Push](#push)
    - [Responce](#responce)
- [Service](#service)
  - [Initialization](#initialization-1)
  - [Service start](#service-start)
- [Utils](#utils)
  - [abstractions](#abstractions)
  - [id_generators](#id_generators)
  - [logger](#logger)
  - [message](#message)
  - [rmq_property](#rmq_property)
  - [storage](#storage)

___

## Short description

***SchedulerGodX*** - a task manager consisting of two modules- a Client and a Service connected by two RabbitMQ queues (client-service, service-client).  Tasks can be either deferred or not. The service module stores the serialized functions (which were passed by the client) in the sqlite database.

> ATTENTION!
>For stable operation, it is advisable to deploy the service module on a machine with a unix-like system.

___ 

## Exemple usage

### Exemple client

```python
import schedulergodx.client as scheduler

client = scheduler.Client()  # client initialization

@client.task
def test(a, b):  # the function passed to the service
    print(a / b)

test.set_parameters(delay=10, task_lifetime=10)  # optional

for i in range(3):  # you can run tasks as many times as you want
    test.launch(10, b=1)  # args and kwargs of the passed function
```

### Exemple service

```python
import schedulergodx.service as scheduler
    
service = scheduler.Service()  # initializing the service
service.start()  # blocking the call to the service start function
```

___

## Client

### Initialization

- ***client = Client(***
   - *name* - the unique name of the client, which he will use to identify himself
   - *core_name* - the name of the client's core module (used for logging)
   - *rmq_connect* - an instance of the **[utils.RmqConnect](#rmq_property)**
   - *id_generator* - a generator that returns a unique id with the str type (defaults: **[utils.id_generators](#id_generators)**)
   - *task_lifetime* - default task lifetime value
   - *hard_task_lifetime* - The default lifetime value of a hard task 
   - *enable_overdue* - whether to complete overdue tasks
***)***
- ***client.logger.< **[utils.LoggerConstructor](#message)** >*** - optional


### Create task

```python 
@client.task
def func(*args, **kwargs) -> None: ...
```

### Task set parametrs

- ***func.set_parameters(***
   - *delay* - delay before execution (no default)
   - *hard* - is the task hard (if True, the service will create a separate process for it). AVAILABLE ONLY WHEN THE SERVICE IS RUNNING ON A UNIX-LIKE SYSTEM!
   - *task_lifetime* - maximum task completion time
   - *hard_task_lifetime* - maximum hard task completion time
***)***


### Task launch

```python
func.launch(args, kwargs)
```

### More methods

- #### Threads

  - ##### get_threads()
  ```python 
  client.get_threads()  # returns all threads ever created
  ``` 

  - ##### get_thread(id)
  ```python 
  client.get_threads()  # returns thread by message id  
  ```  

- #### Push

  - ##### push()
  ```python 
  client.push(data, **kwargs)  # Sends your message to the client-service queue (it is recommended to use the message constructor)
  ``` 
  Message constructor: **[utils.MessageConstructor](#message)**


- #### Responce

  - ##### get_responce()
  ```python 
  client.get_responce(message_id)  # Get a message from the service-client queue by id
  ``` 

  - ##### sync_await_responce()
  ```python 
  client.sync_await_responce(message_id)  # Waiting for a message from the service-client queue (blocking method)
  ``` 

  - ##### async_get_responce()
  ```python 
  client.async_get_responce(message_id)  # Waiting for a message from the service-client queue
  ``` 

  - ##### Exemple:
  ```python
  @client.task
  def test() -> None: ... 
  message_id = test.launch()
  responce = client.sync_await_responce(message_id)
  print(responce.metadata, responce.arguments)
  ```

___

## Service

### Initialization

- ***service = Service(***
   - *name* - the unique name of the service, which he will use to identify himself
   - *core_name* - the name of the service's core module (used for logging)
   - *rmq_connect* - an instance of the **[utils.RmqConnect](#rmq_property)**
   - *id_generator* - a generator that returns a unique id with the str type (defaults: **[utils.id_generators](#id_generators)**)
   - *db* - an instance of the **[utils.DB](#storage)**
***)***

### Service start

```python
service.start()
```

___

## Utils

### abstractions
Contains abstractions from which the library's internal modules are inherited

### id_generators
Contains default id generators
Exemple:
```python
import schedulergodx.client as scheduler
from schedulergodx.utils import id_generators

client = scheduler.Client(
    id_generator = id_generators.autoincrement()
) 
```

### logger
Contains a LoggerConstructor

## message
A module containing constants for messages and MessageConstructor
Exemple:
```python
from schedulergodx.utils import message

message_ = message.MessageConstructor.info(...)
```

## rmq_property
A module containing the rmq connection class and default settings
Excemple:
```python
import schedulergodx.client as scheduler
from schedulergodx.utils import rmq_property

rmq_settings = rmq_property.RmqSettings(
    {
        'host': 'localhost',
        'port': 5672,
        'virtual_host': '/',
        'heartbeat': 60, 
        'blocked_connection_timeout': 300
    },
    (
        'guest', 
        'guest'
    )  # rmq_property.rmq_default_settings stores the same settings
)


client = scheduler.Client(
    rmq_connect = rmq_property.RmqConnect(
        rmq_parameters=rmq_settings.parametrs,
        rmq_credentials=rmq_settings.credentials
    )
) 
```

## storage
A module used to manage the database by internal library modules
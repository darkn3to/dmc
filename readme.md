<h1 align="center">DMC - Distributed Modular Compute</h1>

DMC is a distributed deep learning training pipeline designed to enable collaborative model training across multiple machines using commodity hardware. It focuses on modularity, dataset sharding, adaptive communication, and orchestrated distributed training.

## Features

• <b>Dataset Sharding Pipeline</b> — splits datasets into shards using file-based or logically grouped strategies.

• <b>HRW-based Distribution</b> — assigns shards deterministically across nodes using rendezvous (HRW) hashing.

• <b>Replica Shards</b> — supports redundancy for fault recovery through replica placement.

• <b>PyTorch Distributed Training</b> — leverages PyTorch DDP for stable and scalable training.

• <b>Ansible-based Orchestration</b> — uses Ansible to coordinate multi-node execution.

• <b>Adaptive Communication (AdaComm)</b> — dynamically adjusts gradient synchronization frequency.

• <b>Replication Factor</b> — controls the redundancy of the dataset in the cluster to cater training takover in case of machine crash.

## Working Overview
DMC operates as a pipeline with preprocessing and training stages.

### 1. Sharding (Preprocessing Phase)
The user provides a dataset on a master node.

DMC creates dataset shards using two strategies:

- <b>File-based sharding</b> — for independent files.

- <b>Group-based sharding</b> — preserves logical relationships.

Additional capabilities:
- User-defined shard sizing
- HRW hashing for shard-to-node assignment
- Replica shard placement for redundancy

Each worker node pulls its assigned shards from the master using a pull-based mechanism.

This completes preprocessing.

### 2. Training Pipeline
Training is orchestrated and executed in multiple layers:

- Orchestration Layer
  - Uses Ansible to launch distributed jobs across nodes
  - Initializes training processes via a runner script

- Distributed Environment
  - Runner initializes a PyTorch Distributed (DDP) environment
  - Environment can be torn down and recreated during failures

- Data Loading
  - Custom dataloader loads data from local shards
  - No centralized dataset dependency during training

- Training Execution
  - User wraps optimizer with a DMC abstraction
  - DMC internally uses PyTorch DDP for gradient synchronization

## Adaptive Communication (AdaComm)
DMC implements adaptive gradient communication based on the paper: [Adaptive Communication Strategies for Distributed SGD](https://arxiv.org/abs/1810.08313).

Key idea:

- Early training → large gradients → <b>less frequent communication</b>

- Later training → convergence phase → <b>more frequent communication</b>

Controlled via parameter:

- <b>τ (tau)</b> — number of local steps before synchronization

This reduces network overhead while maintaining convergence quality.

## Fault Tolerance
While not fully implemented, DMC architecture was designed keeping fault tolerance in mind. For example, we use the concept of Original and Replica Shards in order to ease machine training takeover in case of machine crash. The machine with the Replica shards can takeover the training which was originally assigned to the crashed machine. This data redundancy in the system is controlled by Replication Factor. The higher the replication factor, the more the fault tolerance capability. 

## Usage 
1. Setup cluster nodes and ensure SSH + Ansible connectivity.
2. Populate nodes.txt and inventory.ini in the root with the master and worker IP addresses. 
3. Place dataset on master node.
4. Run sharding logic on master node using `sharding_runner.py` to create shards.

   ```cmd
   python3 sharding_runner.py /path/to/dataset --max-shard-size 4 --mode folder --depth 3
   ```
5. Run the puller.py on worker machines. Using:
   ```cmd
   python3 puller.py
   ```
6. Initiate the distributed training using the command:
   ```cmd
   ansible-playbook -i inventory.ini run_dmc.yml
   ```

## Limitations

• Fault tolerance is not yet implemented.

• Requires controlled network environment (LAN preferred).

## Future Scope

• Robust fault tolerance with seamless recovery

• Improved monitoring and metrics

### This project was tested on Ubuntu 22.04.

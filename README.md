# Logstore Project

## Publications
1. (IEEE TKDE)[https://doi.org/10.1109/TKDE.2020.3027191]
2. (ICDE TKDE Poster Track)[https://icde2021.gr/accepted-papers/]

## System Setup

### Google Cloud Setup

### Setting up Disks

Use the following link as a reference:
[GC Add Disk Guide](https://cloud.google.com/compute/docs/disks/add-persistent-disk)

Get the `DEVICE_ID` for the new disk. The new disk should be the SSD. Using the following command.

```shell script
$ sudo lsblk
```

Format using `ext4`:

```shell script
$ sudo mkfs.ext4 -m 0 -E lazy_itable_init=0,lazy_journal_init=0,discard /dev/DEVICE_ID
```

Create mount directories: 
We shall use the directories names as hardcoded in the bench scripts. However, those names can be anything you like given that you also change the values in the benchmarking scripts (i.e., `bench.sh`). 

Create data directory which is used as a last-level DB datafile in LogStore:

```shell script
$ sudo mkdir -p /disks/data
```

Note that the same main disk is used as last level. In our experiments, this is HDD.

Create directory for SSD whihch is used as the main level DB datafile in LogStore.

```shell script
$ sudo mkdir -p /disks/ssd
```

Mount SSD

```shell script
$ sudo mount -o discard,defaults /dev/DEVICE_ID /disks/ssd
```

Set permissions

```shell script
$ sudo chmod a+w /disks/ssd; sudo chmod a+w /disks/data
```

Creat logstore directories
```shell script
$ mkdir -p /disks/ssd/logstore; mkdir -p /disks/data/logstore
```

Creat leveldb directories
```shell script
$ mkdir -p /disks/ssd/leveldb; mkdir -p /disks/data/leveldb
```


## Tutorial (Ubuntu 18.04)

### Set up YCSB dependency

Make sure dependencies are installed. e.g., Maven.
To use the Python scripts, make sure to install the dependencies using `requirements.txt`.
We recommend using VirutalEnv to manage your Python environment.

```shell
$ pip install -r requirements.txt
```

### Install dependencies

```shell script
$ sudo apt-get update; sudo apt install git make build-essential maven -y
```

# Build customized YCSB

Unzip the `YCSB.zip` file. Then, use the following command. 

```shell script
$ cd YCSB; mvn clean package
``` 

## References
1. (YCSB Github)[https://github.com/brianfrankcooper/YCSB/]
2. (LevelDB Github)[https://github.com/google/leveldb]
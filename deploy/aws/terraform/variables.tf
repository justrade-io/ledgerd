variable "region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "ap-southeast-1"
}

variable "project" {
  description = "Project name used as a prefix for resource naming."
  type        = string
  default     = "ledgerd"
}

variable "availability_zone" {
  description = "Availability zone for the POC deployment (single AZ)."
  type        = string
  default     = "ap-southeast-1a"
}

variable "cidr_block" {
  description = "VPC CIDR block."
  type        = string
  default     = "10.0.0.0/16"
}

variable "subnet_cidr" {
  description = "Public subnet CIDR block."
  type        = string
  default     = "10.0.1.0/24"
}

variable "node_count" {
  description = "Number of Raft cluster nodes."
  type        = number
  default     = 3
}

variable "node_instance_type" {
  description = "EC2 instance type for cluster nodes."
  type        = string
  default     = "c6i.xlarge"
}

variable "read_instance_type" {
  description = "EC2 instance type for the read replica."
  type        = string
  default     = "c6i.xlarge"
}

variable "bench_instance_type" {
  description = "EC2 instance type for the load/bench driver."
  type        = string
  default     = "c6i.xlarge"
}

variable "monitor_instance_type" {
  description = "EC2 instance type for Prometheus + Grafana (used only when split_bench_monitor is true)."
  type        = string
  default     = "c6i.xlarge"
}

variable "split_bench_monitor" {
  description = "Run the load driver and the monitoring stack on separate instances."
  type        = bool
  default     = false
}

variable "data_volume_size" {
  description = "Size in GB of each node's persistent data EBS volume."
  type        = number
  default     = 20
}

variable "operator_cidr" {
  description = "CIDR allowed to SSH and reach Grafana (3000) / Prometheus (9090)."
  type        = string
  default     = "0.0.0.0/0"
}

variable "instance_ami" {
  description = "AMI id. Leave empty to auto-select the latest Ubuntu 22.04 LTS."
  type        = string
  default     = ""
}

variable "ssh_public_key" {
  description = "Path to an existing SSH public key. Leave empty to generate an ED25519 key pair."
  type        = string
  default     = ""
}

variable "ssh_private_key_path" {
  description = "Where the generated private key is written (POC only). Ignored when ssh_public_key is set."
  type        = string
  default     = "../.ssh/ledgerd-key.pem"
}

variable "tags" {
  description = "Additional tags applied to every resource."
  type        = map(string)
  default     = {}
}

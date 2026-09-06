terraform {
  required_version = ">= 1.3"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.0"
    }
  }
}

provider "aws" {
  region = var.region
}

# Latest Ubuntu 22.04 LTS (Jammy) HVM image from Canonical.
data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"]

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

locals {
  ami = var.instance_ami != "" ? var.instance_ami : data.aws_ami.ubuntu.id

  common_tags = merge(var.tags, { Project = var.project })

  # Aeron member string: id,ingress,consensus,log,catchup,archive joined by '|'.
  # Ports follow ClusterConfig.PORT_BASE (20100) + node_id * PORT_STRIDE (100).
  cluster_members = join("|", [
    for i in range(var.node_count) : format(
      "%d,%s:%d,%s:%d,%s:%d,%s:%d,%s:%d",
      i,
      aws_instance.node[i].private_ip, 20100 + i * 100,
      aws_instance.node[i].private_ip, 20100 + i * 100 + 1,
      aws_instance.node[i].private_ip, 20100 + i * 100 + 2,
      aws_instance.node[i].private_ip, 20100 + i * 100 + 3,
      aws_instance.node[i].private_ip, 20100 + i * 100 + 4,
    )
  ])

  # WriteClient ingress endpoints: "0=ip:20100,1=ip:20200,..."
  ingress_endpoints = join(",", [
    for i in range(var.node_count) : format("%d=%s:%d", i, aws_instance.node[i].private_ip, 20100 + i * 100)
  ])

  # Read replica Archive control channels, one per member (ADR 0008).
  archive_channels = join(",", [
    for i in range(var.node_count) : format("aeron:udp?endpoint=%s:%d", aws_instance.node[i].private_ip, 20100 + i * 100 + 4)
  ])
}

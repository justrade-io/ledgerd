resource "aws_instance" "node" {
  count                       = var.node_count
  ami                         = local.ami
  instance_type               = var.node_instance_type
  subnet_id                   = aws_subnet.public.id
  vpc_security_group_ids      = [aws_security_group.ledgerd.id]
  key_name                    = aws_key_pair.ledgerd.key_name
  associate_public_ip_address = true

  root_block_device {
    volume_size = 20
    volume_type = "gp3"
  }

  tags = merge(local.common_tags, {
    Name = "${var.project}-node-${count.index}"
    Role = "node"
  })
}

resource "aws_instance" "read" {
  ami                         = local.ami
  instance_type               = var.read_instance_type
  subnet_id                   = aws_subnet.public.id
  vpc_security_group_ids      = [aws_security_group.ledgerd.id]
  key_name                    = aws_key_pair.ledgerd.key_name
  associate_public_ip_address = true

  root_block_device {
    volume_size = 20
    volume_type = "gp3"
  }

  tags = merge(local.common_tags, {
    Name = "${var.project}-read-0"
    Role = "read"
  })
}

resource "aws_instance" "bench" {
  ami                         = local.ami
  instance_type               = var.bench_instance_type
  subnet_id                   = aws_subnet.public.id
  vpc_security_group_ids      = [aws_security_group.ledgerd.id]
  key_name                    = aws_key_pair.ledgerd.key_name
  associate_public_ip_address = true

  root_block_device {
    volume_size = 20
    volume_type = "gp3"
  }

  tags = merge(local.common_tags, {
    Name = "${var.project}-bench"
    Role = "bench"
  })
}

resource "aws_instance" "monitor" {
  count                       = var.split_bench_monitor ? 1 : 0
  ami                         = local.ami
  instance_type               = var.monitor_instance_type
  subnet_id                   = aws_subnet.public.id
  vpc_security_group_ids      = [aws_security_group.ledgerd.id]
  key_name                    = aws_key_pair.ledgerd.key_name
  associate_public_ip_address = true

  root_block_device {
    volume_size = 20
    volume_type = "gp3"
  }

  tags = merge(local.common_tags, {
    Name = "${var.project}-monitor"
    Role = "monitor"
  })
}

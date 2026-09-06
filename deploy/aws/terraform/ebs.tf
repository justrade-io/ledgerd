# Each node keeps its Aeron Archive + cluster state on a dedicated EBS volume so
# a node can restart with cleanStart=false and recover from its own snapshot/log.
# The read replica and bench/monitor are stateless and use their root volume.

resource "aws_ebs_volume" "node_data" {
  count             = var.node_count
  availability_zone = var.availability_zone
  size              = var.data_volume_size
  type              = "gp3"

  tags = merge(local.common_tags, { Name = "${var.project}-node-${count.index}-data" })
}

resource "aws_volume_attachment" "node_data" {
  count       = var.node_count
  device_name = "/dev/sdf"
  volume_id   = aws_ebs_volume.node_data[count.index].id
  instance_id = aws_instance.node[count.index].id
}

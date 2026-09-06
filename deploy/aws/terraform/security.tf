# A single self-referencing security group covers every cluster member (nodes,
# read replica, bench driver, monitor). Aeron uses UDP unicast in both directions
# between members and clients, so intra-SG traffic must be fully open. Operator
# access (SSH, Grafana, Prometheus) is limited to operator_cidr.

resource "aws_security_group" "ledgerd" {
  name        = "${var.project}-sg"
  description = "LEDGERD cluster self-referencing SG plus operator access"
  vpc_id      = aws_vpc.main.id

  tags = merge(local.common_tags, { Name = "${var.project}-sg" })
}

resource "aws_security_group_rule" "self_ingress" {
  type              = "ingress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  self              = true
  security_group_id = aws_security_group.ledgerd.id
}

resource "aws_security_group_rule" "ssh" {
  type              = "ingress"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = [var.operator_cidr]
  security_group_id = aws_security_group.ledgerd.id
}

resource "aws_security_group_rule" "grafana" {
  type              = "ingress"
  from_port         = 3000
  to_port           = 3000
  protocol          = "tcp"
  cidr_blocks       = [var.operator_cidr]
  security_group_id = aws_security_group.ledgerd.id
}

resource "aws_security_group_rule" "prometheus" {
  type              = "ingress"
  from_port         = 9090
  to_port           = 9090
  protocol          = "tcp"
  cidr_blocks       = [var.operator_cidr]
  security_group_id = aws_security_group.ledgerd.id
}

resource "aws_security_group_rule" "egress_all" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_security_group.ledgerd.id
}

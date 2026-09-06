# POC key pair. If ssh_public_key is supplied it is used directly; otherwise an
# ED25519 key pair is generated and the private key is written locally (and must
# never be committed - see deploy/aws/.gitignore).

resource "tls_private_key" "ssh" {
  count     = var.ssh_public_key == "" ? 1 : 0
  algorithm = "ED25519"
}

resource "aws_key_pair" "ledgerd" {
  key_name   = "${var.project}-key"
  public_key = var.ssh_public_key != "" ? file(var.ssh_public_key) : tls_private_key.ssh[0].public_key_openssh

  tags = local.common_tags
}

resource "local_file" "private_key" {
  count           = var.ssh_public_key == "" ? 1 : 0
  content         = tls_private_key.ssh[0].private_key_openssh
  filename        = abspath(var.ssh_private_key_path)
  file_permission = "0600"
}

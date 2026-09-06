output "node_private_ips" {
  description = "Private IPs of the cluster nodes, indexed by node id."
  value       = aws_instance.node[*].private_ip
}

output "node_public_ips" {
  description = "Public IPs of the cluster nodes."
  value       = aws_instance.node[*].public_ip
}

output "read_private_ip" {
  description = "Private IP of the read replica."
  value       = aws_instance.read.private_ip
}

output "bench_private_ip" {
  description = "Private IP of the bench driver."
  value       = aws_instance.bench.private_ip
}

output "bench_public_ip" {
  description = "Public IP of the bench driver."
  value       = aws_instance.bench.public_ip
}

output "monitor_public_ip" {
  description = "Public IP of the host running Prometheus + Grafana."
  value       = var.split_bench_monitor ? aws_instance.monitor[0].public_ip : aws_instance.bench.public_ip
}

output "grafana_url" {
  description = "Grafana URL."
  value       = "http://${var.split_bench_monitor ? aws_instance.monitor[0].public_ip : aws_instance.bench.public_ip}:3000"
}

output "key_pair_name" {
  description = "EC2 key pair name."
  value       = aws_key_pair.ledgerd.key_name
}

output "cluster_members" {
  description = "Aeron cluster members string."
  value       = local.cluster_members
  sensitive   = true
}

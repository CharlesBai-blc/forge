output "instance_ids" {
  value = aws_instance.worker[*].id
}

output "public_ips" {
  value = aws_instance.worker[*].public_ip
}

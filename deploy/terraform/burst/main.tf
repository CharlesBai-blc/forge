# Ubuntu 24.04 arm64 AMI via Canonical's public SSM parameter, so the
# module needs no hardcoded per-region AMI IDs.
data "aws_ssm_parameter" "ubuntu" {
  name = "/aws/service/canonical/ubuntu/server/24.04/stable/current/arm64/hvm/ebs-gp3/ami-id"
}

# Workers only dial out to the control plane; no inbound access.
resource "aws_security_group" "burst" {
  name_prefix = "forge-burst-"
  description = "forge burst workers: egress only"

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "worker" {
  count = var.instance_count

  ami                    = data.aws_ssm_parameter.ubuntu.insecure_value
  instance_type          = var.instance_type
  vpc_security_group_ids = [aws_security_group.burst.id]

  dynamic "instance_market_options" {
    for_each = var.spot ? [1] : []
    content {
      market_type = "spot"
      spot_options {
        spot_instance_type             = "one-time"
        instance_interruption_behavior = "terminate"
      }
    }
  }

  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    control_plane_url  = var.control_plane_url
    enroll_token       = var.enroll_token
    agent_download_url = var.agent_download_url
  })

  # Enrollment tokens are single-use (FR-3): a new token in user_data
  # must not recreate already-enrolled instances. Only instances added
  # by the current apply see the current token.
  lifecycle {
    ignore_changes = [user_data]
  }

  tags = {
    Name = "forge-burst-${count.index}"
  }
}

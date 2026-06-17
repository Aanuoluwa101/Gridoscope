terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

# No inline ingress rules — the ECS tasks SG and the MSK Connect SG both need
# to reach this on 9098, but creating those rules here would require this
# module to take both SG ids as inputs while ecs_cluster/msk_connect take
# this SG's id as an input in turn, which is a module dependency cycle.
# The cross-SG rules are added from the root module instead, where both
# sides are already visible.
resource "aws_security_group" "msk" {
  name        = "gridoscope-${var.environment}-msk"
  description = "Gridoscope MSK brokers — IAM-auth Kafka port, ingress wired from root"
  vpc_id      = var.vpc_id

  egress {
    description = "All outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "gridoscope-${var.environment}-msk"
  }
}

resource "aws_msk_cluster" "this" {
  cluster_name           = "gridoscope-${var.environment}"
  kafka_version          = var.kafka_version
  number_of_broker_nodes = var.broker_count

  broker_node_group_info {
    instance_type   = var.broker_instance_type
    client_subnets  = var.private_subnet_ids
    security_groups = [aws_security_group.msk.id]

    storage_info {
      ebs_storage_info {
        volume_size = var.ebs_volume_size
      }
    }
  }

  # IAM auth requires TLS on the broker listener — there is no plaintext
  # fallback here, matching the producer/consumer IAM auth decision.
  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  client_authentication {
    sasl {
      iam = true
    }
  }

  tags = {
    Name = "gridoscope-${var.environment}-msk"
  }
}

# Resolves the actual broker connection string for producers/consumers —
# only known after the cluster exists, so it has to be a data source, not
# a literal.
data "aws_msk_bootstrap_brokers" "this" {
  cluster_arn = aws_msk_cluster.this.arn
}

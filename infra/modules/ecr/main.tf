terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

resource "aws_ecr_repository" "producer" {
  name                 = "gridoscope-${var.environment}-producer"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = {
    Name = "gridoscope-${var.environment}-producer"
  }
}

resource "aws_ecr_repository" "consumer" {
  name                 = "gridoscope-${var.environment}-consumer"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = {
    Name = "gridoscope-${var.environment}-consumer"
  }
}

# Keep only the last 10 images per repo — this project runs in short bursts,
# no need to accumulate untagged build history indefinitely.
resource "aws_ecr_lifecycle_policy" "producer" {
  repository = aws_ecr_repository.producer.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep last 10 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 10
      }
      action = { type = "expire" }
    }]
  })
}

resource "aws_ecr_lifecycle_policy" "consumer" {
  repository = aws_ecr_repository.consumer.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep last 10 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 10
      }
      action = { type = "expire" }
    }]
  })
}

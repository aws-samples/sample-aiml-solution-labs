---
title: "2. Deploy"
weight: 42
---

**Time:** 10 minutes | **Where:** Control Plane | **User:** Alice

This time you deploy with **Terraform**, the second deploy option. It creates the same kind of Execution Plane as AWS CDK, and the Control Plane keeps its Terraform state for you.

1. In the **Deployment** panel, choose **Deploy** and set these values:

   | Field | Value |
   |---|---|
   | **Deploy to** | This console's account |
   | **Region** | `us-east-1` |
   | **Deploy with** | **Terraform** |

2. The deploy takes about 7 minutes. While it runs, read [Under the hood](/04-content-studio/03-under-the-hood).
3. When the panel shows **Version 1 deployed with Terraform**, choose **Show the temporary password**, then **Open app**, and sign in as `alice@example.com`.

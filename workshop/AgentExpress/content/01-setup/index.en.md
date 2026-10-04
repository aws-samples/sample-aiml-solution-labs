---
title: "Set up"
weight: 10
---

**Time:** 5 minutes (plus about 10 minutes to deploy in your own account) | **Where:** your browser

Everything in this workshop happens in the **AgentExpress Control Plane**, a web console where you design and deploy workflows. How you get to it depends on where you run the workshop:

- **At an AWS event**, everything is already set up in your workshop account: the Control Plane is deployed, both users are created, and model access is checked. You copy the URL and password, and sign in.
- **In your own account**, you first deploy the Control Plane with one CloudFormation template, which takes about 10 minutes. Then you sign in the same way.

You work as two users, so you can see collaboration from both sides:

- **Alice** designs, deploys and runs the workflows. You are Alice for most of the workshop.
- **Bob** is a colleague. Alice shares a workflow with him in Lab 1, and he edits it.

| Step | What you do |
|---|---|
| [Get your Control Plane](/01-setup/01-deploy-console) | Copy the Control Plane URL and the password. In your own account, deploy the Control Plane first. |
| [Sign in](/01-setup/02-sign-in) | Sign in as Alice and find your way around. |

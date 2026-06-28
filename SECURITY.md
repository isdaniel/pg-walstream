# Security Policy

## Supported Versions

We provide security fixes for the latest published release on the `0.6.x` line.
Older versions may not receive patches — please upgrade to the latest release.

| Version | Supported          |
| ------- | ------------------ |
| 0.6.x   | :white_check_mark: |
| < 0.6   | :x:                |

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

Instead, report them privately via one of the following:

- GitHub's [private vulnerability reporting](https://github.com/isdaniel/pg-walstream/security/advisories/new)
  (preferred), or
- Email **dog830228@gmail.com** with the subject line `SECURITY: pg-walstream`.

Please include:

- A description of the vulnerability and its impact.
- Steps to reproduce or a proof of concept.
- The affected version(s) and connection backend (libpq or rustls-tls).

## What to expect

- We aim to acknowledge your report within **72 hours**.
- We will work with you to understand and validate the issue.
- Once a fix is ready, we will publish a new release and credit you in the
  advisory (unless you prefer to remain anonymous).

Thank you for helping keep pg_walstream and its users safe.

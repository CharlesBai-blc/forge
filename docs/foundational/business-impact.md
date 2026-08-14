# Forge: Business Impact Statement

## Problem

Every software team uses an automatic checker: each time someone saves new code, a robot runs the project's tests to catch mistakes before they reach customers. Most teams rent the computers that robot runs on, GitHub charges by the minute, and the rented machines are deliberately modest. As a project grows, two costs climb together: the monthly bill, and the time every developer spends waiting for slow test runs. Waiting is the expensive one, it is idle engineer time, multiplied across every code change, every day. Meanwhile, powerful computers those same teams already own, a beefy desktop, a spare server, hardware in a closet, sit idle, because pointing the robot at them is genuinely difficult today.

## Audience

- **Small teams and independent developers** overhead: per-minute charges on heavy test suites turn into a real monthly line item.
- **Growing teams** speed: slow checks stretch every code review and every release.
- **Security and privacy-sensitive teams** control: their code and secrets run on computers someone else operates.

## Proof

Several venture-funded startups, Depot, Blacksmith, Namespace, BuildJet, exist purely to sell "the same test-running, but faster or cheaper." Companies pay them today to escape rented-runner economics. A market of funded companies is the strongest evidence a pain point is real; Forge addresses the same pain with ownership instead of another rental.

## Features

Forge is one program you install on computers you already own. After a ten-minute setup, your project's automatic checks run on your hardware instead of rented machines, typically several times faster, at no per-minute cost. Each check runs inside a fresh, disposable compartment that is destroyed afterward, so one bad or malicious code change can never contaminate the next. And when your own machines are all busy, Forge temporarily borrows cloud computers, uses them, and gives them back, you get the reliability of the cloud only in the moments you need it.

## Impact

- **Money:** per-minute CI charges drop to roughly zero for teams with usable hardware; the cloud is paid for only during overflow minutes.
- **Time:** a modern 16-core machine you own is several times faster than the standard rented 2-core runner. Every code change gets its answer sooner, which shortens every review and release across the whole team, every day.
- **Control:** code, tests, and secrets stay on machines you own. Nothing about your build leaves your infrastructure unless you choose to burst.
- **Simplicity:** the existing do-it-yourself route requires running Kubernetes, a heavyweight system that is a job in itself. Forge's bar is one binary and one config file.




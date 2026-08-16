# FUTURE

Out-of-scope ideas recorded per the charter. Not commitments.

- **Opt-in run-cancel on dead-letter.** When a never-acquired job dead-letters, optionally cancel its GitHub workflow run (`POST /repos/{owner}/{repo}/actions/runs/{run_id}/cancel`) so the run does not sit queued until GitHub's timeout. Off by default: cancelling a run cancels sibling jobs in the same run, including matrix jobs running on other workers (FR-12 v1.3).

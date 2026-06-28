## Summary

<!-- What does this PR do and why? -->

## Related issues

<!-- e.g. Closes #123 -->

## Type of change

- [ ] Bug fix
- [ ] New feature
- [ ] Performance improvement
- [ ] Refactor / internal cleanup
- [ ] Documentation
- [ ] CI / build

## Checklist

- [ ] `make before-git-push` passes locally (check, build, format, audit, test, doc-check)
- [ ] Tests added/updated for the change (coverage stays ≥ 90%)
- [ ] Both backends build (`--features rustls-tls` if connection/streaming code changed)
- [ ] Docs / README updated if public API changed

## Performance notes

<!-- If this touches the hot path (protocol parsing / streaming), note any benchmark
     results from `cargo bench --bench wal_pipeline`. Otherwise write "n/a". -->

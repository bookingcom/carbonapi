Contribution guidelines
=======================

**All contibutions happen via pull-requests.**

## Making a pull-request

- Before making a more complex pull-request make sure to open an issue beforehand if it does not exist yet. This allows for a preliminary discussion before the pull-request is initially submitted. This often eliminates unnecessary work.
- Please, mark the PR as WIP if you plan to add some work in the future.
- Pull-request requires at least one approval to be merged into `master` branch.
- Refer to relevant issue if it's there.
- Make sure the conflicts with `master` are resolved.
- The author of the pull-request is responsible for merging it if they have sufficient privileges. This makes sense because sometimes there are conflicts.

## Code style

- [CodeReviewComments](https://github.com/golang/go/wiki/CodeReviewComments)
- [Effective Go](https://golang.org/doc/effective_go.html)
- [Peter Bourgon's style guide](https://peter.bourgon.org/go-in-production/#formatting-and-style)

## Issues

- Please, check if some issues about your topic already exist before making a new issue. Continue the discussion in the old issue if it's there, or refer to it.

## Checking if things work

- It is encouraged to play around and check if the new features work and nothing is broken before submitting the pull-request.
- The best way to do this frequently is to run the `docker-compose` as described in the readme.

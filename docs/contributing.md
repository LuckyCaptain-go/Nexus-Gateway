# Contributing Guide

## Overview

Thank you for your interest in contributing to Nexus-Gateway! We welcome contributions from the community and appreciate your efforts to improve the project. This guide outlines the process for contributing to the project.

## How to Contribute

### Reporting Issues

Before creating an issue, please:
1. Search existing issues to avoid duplicates
2. Check the documentation to see if your question is answered
3. Provide a clear and descriptive title
4. Include steps to reproduce bugs
5. Specify your environment (OS, Go version, etc.)

### Feature Requests

When suggesting new features, please:
1. Explain the problem you're trying to solve
2. Describe your proposed solution
3. Discuss potential alternatives
4. Indicate if you're willing to implement the feature

### Pull Requests

We welcome pull requests for bug fixes, new features, and documentation improvements. Here's how to submit a quality pull request:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Make your changes
4. Add tests if applicable
5. Update documentation as needed
6. Ensure all tests pass (`go test ./...`)
7. Commit your changes with a clear message
8. Push to your fork (`git push origin feature/AmazingFeature`)
9. Open a pull request

## Development Setup

### Prerequisites

- Go 1.25 or higher
- Git
- Make (optional)

### Getting Started

1. Fork and clone the repository:
   ```bash
   git clone https://github.com/YOUR_USERNAME/Nexus-Gateway.git
   cd Nexus-Gateway
   ```

2. Install dependencies:
   ```bash
   go mod download
   ```

3. Create a feature branch:
   ```bash
   git checkout -b feature/your-feature-name
   ```

4. Run tests to ensure everything works:
   ```bash
   go test ./...
   ```

## Code Style

### Go Code Standards

- Follow the official Go formatting guidelines (gofmt)
- Use meaningful variable and function names
- Write clear, concise comments
- Document exported functions and types
- Follow the project's error handling patterns

### Naming Conventions

- Use camelCase for variable and function names
- Use PascalCase for exported types and functions
- Use descriptive names that reflect purpose
- Follow Go idioms and patterns

### Error Handling

```go
// Good pattern
if err := someOperation(); err != nil {
    return fmt.Errorf("failed to perform operation: %w", err)
}
```

## Types of Contributions

### Bug Fixes

1. Create an issue describing the bug if one doesn't exist
2. Fork the repository
3. Create a fix branch
4. Write a test case that reproduces the bug
5. Implement the fix
6. Ensure all tests pass
7. Submit a pull request

### New Features

1. Create an issue to discuss the feature before implementing
2. Fork the repository
3. Create a feature branch
4. Implement the feature
5. Add comprehensive tests
6. Update documentation
7. Ensure all tests pass
8. Submit a pull request

### Documentation

Documentation improvements are always welcome! This includes:
- Fixing typos or grammatical errors
- Clarifying unclear explanations
- Adding examples
- Updating outdated information

### Tests

Help improve test coverage by:
- Writing unit tests for untested code
- Adding integration tests
- Improving existing test cases
- Identifying edge cases to test

## Adding New Database Drivers

If you're interested in adding support for a new database, please follow our [Development Guide](development.md)'s section on adding new drivers.

### Driver Development Checklist

- [ ] Implement the complete `Driver` interface
- [ ] Add comprehensive error handling
- [ ] Include connection validation
- [ ] Handle authentication securely
- [ ] Test with real database instances
- [ ] Add unit tests
- [ ] Update documentation
- [ ] Register the driver in the registry
- [ ] Add the database type to constants

## Testing

### Running Tests

Run all tests:
```bash
go test ./...
```

Run tests with verbose output:
```bash
go test -v ./...
```

Run tests with race detection:
```bash
go test -race ./...
```

### Writing Tests

When adding new functionality, please include tests:
- Unit tests for individual functions
- Integration tests for feature workflows
- Error condition tests
- Performance tests where applicable

## Pull Request Review Process

1. Ensure your PR passes all automated checks
2. A maintainer will review your code
3. Address any feedback provided
4. Once approved, your PR will be merged

### Code Review Checklist

During review, we'll check for:
- Correctness of implementation
- Adherence to code style
- Proper error handling
- Security considerations
- Performance implications
- Test coverage
- Documentation updates
- Breaking changes (if any)

## Community Guidelines

### Be Respectful

- Treat everyone with respect regardless of their experience level
- Provide constructive feedback
- Accept feedback gracefully
- Be patient with newcomers

### Be Clear

- Write clear commit messages
- Provide detailed descriptions in PRs
- Ask questions when uncertain
- Document decisions

### Be Collaborative

- Work together to solve problems
- Offer help to other contributors
- Share knowledge freely
- Welcome different perspectives

## Getting Help

If you need help with your contribution:
- Open an issue for technical questions
- Check the documentation
- Look at existing examples in the codebase
- Reach out to maintainers via GitHub

## Recognition

All contributors are recognized in our acknowledgments. Major contributors may be invited to join the maintainer team.

Thank you for contributing to Nexus-Gateway!
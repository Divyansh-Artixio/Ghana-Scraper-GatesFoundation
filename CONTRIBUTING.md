# Contributing to Ghana FDA Regulatory Scraper

Thank you for your interest in contributing to this project! This document provides guidelines and information for contributors.

## 🚀 Getting Started

### Development Setup

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/Ghana.git
   cd Ghana
   ```
3. **Set up development environment**:
   ```bash
   python setup.py  # Run the setup script
   # OR manually:
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   playwright install
   ```
4. **Create a branch** for your feature:
   ```bash
   git checkout -b feature/your-feature-name
   ```

### Database Setup for Development

```bash
# Create test database
createdb ghana_regulatory_dev

# Run schema
psql -d ghana_regulatory_dev -f schema.sql

# Test connection
python test_db_connection.py
```

## 🧪 Testing

### Running Tests

```bash
# Test database connection
python test_db_connection.py

# Test with limited scraping
SCRAPE_LIMIT=2 python run_scraper.py

# Check results
python check_db_results.py
```

### Adding New Tests

When adding new functionality, please include appropriate tests:

1. **Database tests** in `test_db_connection.py`
2. **Scraper tests** with small limits
3. **Utility function tests** in separate test files

## 📝 Code Style

### Python Style Guidelines

- Follow **PEP 8** style guidelines
- Use **type hints** where appropriate
- Add **docstrings** for all functions and classes
- Keep functions **focused and small**
- Use **meaningful variable names**

### Example Code Style

```python
def extract_company_data(content: str, company_type: str) -> Optional[Dict[str, Any]]:
    """
    Extract company information from content.
    
    Args:
        content: Raw text content to parse
        company_type: Type of company (Manufacturer/Reselling Firm)
        
    Returns:
        Dictionary with company data or None if not found
    """
    if not content or not company_type:
        return None
    
    # Implementation here
    return company_data
```

### Logging

Use the existing logging framework:

```python
import logging
logger = logging.getLogger(__name__)

# Use appropriate log levels
logger.info("Processing started")
logger.warning("Potential issue detected")
logger.error("Failed to process item")
logger.debug("Detailed debug information")
```

## 🔧 Areas for Contribution

### High Priority

1. **Error Handling**: Improve robustness for edge cases
2. **Performance**: Optimize scraping speed and memory usage
3. **Data Quality**: Enhance data validation and cleaning
4. **Documentation**: Improve code documentation and examples

### Medium Priority

1. **New Content Types**: Add support for additional regulatory content
2. **AI Enhancement**: Improve company enrichment accuracy
3. **Monitoring**: Add health checks and alerting
4. **API Integration**: Create REST API for data access

### Low Priority

1. **UI Dashboard**: Web interface for viewing data
2. **Scheduling**: Automated periodic scraping
3. **Export Features**: Additional data export formats
4. **Internationalization**: Support for multiple languages

## 🐛 Bug Reports

When reporting bugs, please include:

1. **Clear description** of the issue
2. **Steps to reproduce** the problem
3. **Expected vs actual behavior**
4. **Environment details** (OS, Python version, etc.)
5. **Log files** if available
6. **Screenshots** if applicable

### Bug Report Template

```markdown
**Bug Description**
A clear description of what the bug is.

**To Reproduce**
Steps to reproduce the behavior:
1. Run command '...'
2. See error

**Expected Behavior**
What you expected to happen.

**Environment**
- OS: [e.g. macOS 12.0]
- Python: [e.g. 3.9.7]
- PostgreSQL: [e.g. 13.4]

**Logs**
```
Paste relevant log output here
```
```

## 💡 Feature Requests

For new features, please:

1. **Check existing issues** to avoid duplicates
2. **Describe the use case** clearly
3. **Explain the expected behavior**
4. **Consider implementation complexity**
5. **Discuss potential breaking changes**

## 📋 Pull Request Process

### Before Submitting

1. **Test your changes** thoroughly
2. **Update documentation** if needed
3. **Add/update tests** for new functionality
4. **Check code style** and formatting
5. **Update CHANGELOG.md** if applicable

### Pull Request Template

```markdown
**Description**
Brief description of changes made.

**Type of Change**
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

**Testing**
- [ ] Tests pass locally
- [ ] Added tests for new functionality
- [ ] Manual testing completed

**Checklist**
- [ ] Code follows style guidelines
- [ ] Self-review completed
- [ ] Documentation updated
- [ ] No breaking changes (or clearly documented)
```

### Review Process

1. **Automated checks** must pass (CI/CD)
2. **Code review** by maintainers
3. **Testing** in development environment
4. **Approval** and merge

## 🏗️ Architecture Guidelines

### File Organization

```
Ghana/
├── core/                    # Core scraping logic
│   ├── scrapers/           # Individual scrapers
│   ├── processors/         # Data processors
│   └── utils/              # Utility functions
├── database/               # Database operations
├── tests/                  # Test files
├── docs/                   # Documentation
└── scripts/                # Utility scripts
```

### Database Design

- **Normalized structure** for data integrity
- **Foreign key relationships** for data consistency
- **Indexes** for query performance
- **Constraints** for data validation

### Error Handling Strategy

1. **Graceful degradation** - continue processing other items
2. **Comprehensive logging** - log all errors with context
3. **Retry mechanisms** - for transient failures
4. **Fallback options** - alternative data sources/methods

## 📚 Resources

### Useful Links

- [FDA Ghana Website](https://fdaghana.gov.gh/)
- [Playwright Documentation](https://playwright.dev/python/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [BeautifulSoup Documentation](https://www.crummy.com/software/BeautifulSoup/bs4/doc/)

### Development Tools

- **IDE**: VS Code with Python extension
- **Database**: pgAdmin or DBeaver for database management
- **Testing**: pytest for unit testing
- **Linting**: flake8 or pylint for code quality

## 🤝 Community

### Communication

- **GitHub Issues**: For bugs and feature requests
- **GitHub Discussions**: For questions and general discussion
- **Pull Requests**: For code contributions

### Code of Conduct

- Be **respectful** and **inclusive**
- **Help others** learn and contribute
- **Focus on constructive feedback**
- **Respect different perspectives**

## 📄 License

By contributing to this project, you agree that your contributions will be licensed under the same license as the project.

---

Thank you for contributing to the Ghana FDA Regulatory Scraper! 🇬🇭
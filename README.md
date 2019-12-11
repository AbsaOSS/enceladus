# Enceladus's Github Pages branch

This branch's sole purpose is to serve as Documentation for the Enceladus project.

### Dependencies:
- ruby >= 2.3.0

### To build locally:
```bash
# In the root of the project
$> git checkout gh-pages
$> bundle install
$> bundle exec jekyll serve
# => Now browse to http://localhost:4000
```

### Run convinience scripts

#### Generate new docs
```ruby
ruby create_docs.rb <version>
```

#### Generate release notes
```bash
Usage: ruby get_release_notes.rb [options]

Specific options:
        --github-token TOKEN         Github token.
        --zenhub-token TOKEN         Zenhub token. This means we will use Release object for release notes.
    -z, --use-zenhub                 Run using zenhub. IT needs environment variable ZENHUB_TOKEN. If you use --zenhub-token option, you don't need to use this. This means we will use Release object for release notes.
    -v, --version VERSION            Version of release notes
        --organization ORGANIZATION  Github Organization
        --repository REPOSITORY      Github Repository name
        --repository-id REPOSITORYID Zenhub Repository ID
        --zenhub-url ZENURL          Zenhub API URL
        --github-url GITURL          Github API URL
    -p, --print_empty                Github API URL
    -h, --help                       Show this message
```

# Copyright 2018-2019 ABSA Group Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'optparse'

class OptParser

  def self.options
    @@options
  end

  def self.parse(args)
    options = OpenStruct.new
    options.use_zenhub = false
    options.print_empty = true
    options.organization = 'AbsaOss'
    options.repository = 'enceladus'
    options.repository_id = '154513089' # ENCELADUS REPO ID
    options.zenhub_url = 'https://api.zenhub.io'
    options.github_url = "https://api.github.com/repos/#{options.organization}/#{options.repository}"
    options.github_token = ENV['GITHUB_TOKEN']
    options.zenhub_token = ENV['ZENHUB_TOKEN']

    opt_parser = OptionParser.new do |opts|
      opts.banner = "Usage: get_release_notes.rb [options]"

      opts.separator ""
      opts.separator "Specific options:"

      opts.on("--github-token TOKEN", 'Github token.') do |gt|
        options.github_token = "token #{gt}"
      end

      opts.on("--zenhub-token TOKEN", 'Zenhub token. This means we will use ' +
                                      'Release object for release notes.') do |zt|
        options.use_zenhub = true
        options.zenhub_token = zt
      end

      opts.on("-z", "--use-zenhub", 'Run using zenhub. IT needs environment variable ZENHUB_TOKEN.' +
                                    ' If you use --zenhub-token option, you don\'t need to use this.' +
                                    ' This means we will use Release object for release notes.') do |z|
        if options.zenhub_token.nil? || options.zenhub_token.empty?
          raise ArgumentError, "Can't find ZENHUB_TOKEN environemnt variable", caller
        end
        options.use_zenhub = z
        options.zenhub_token = options.zenhub_token
      end

      opts.on('-v', '--version VERSION', 'Version of release notes') do |v|
        unless v =~ /[0-9]+\.[0-9]+\.[0-9]+/
          raise OptionParser::InvalidArgument, 'Wrong version format', caller
        end
        options.version = v
      end

      opts.on('--organization ORGANIZATION', 'Github Organization') do |org|
        options.organization = org
      end

      opts.on('--repository REPOSITORY', 'Github Repository name') do |repo|
        options.repository = repo
      end

      opts.on('--repository-id REPOSITORYID', 'Zenhub Repository ID') do |repo_id|
        options.repository_id = repo_id
      end

      opts.on('--zenhub-url ZENURL', 'Zenhub API URL') do |url|
        options.zenhub_url = url
      end

      opts.on('--github-url GITURL', 'Github API URL') do |url|
        options.github_url = url
      end

      opts.on('-p', '--print_empty', 'Github API URL') do |p|
        options.print_empty = p
      end

      opts.on_tail("-h", "--help", "Show this message") do
        puts opts
        exit
      end
    end

    opt_parser.parse!(args)

    raise OptionParser::MissingArgument, 'Missing version argument', caller if options[:version].nil?
    if options.github_token.nil? || options.github_token.empty?
      raise OptionParser::MissingArgument, 'Missing Github token argument or environment variable', caller
    end
    @@options = options
    options
  end
end

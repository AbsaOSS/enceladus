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

class Release
  attr_reader :id, :title, :description, :issues

  def initialize(id:, title:, description:)
    @id = id
    @title = title
    @description = description
  end

  def get_issues(issues:)
    @issues = issues.inject([]) { |acc, v| acc << Issue.new(number: v[:number], title: v[:title]) }
    @issues
  end
end

class ZenhubRelease < Release
  def self.create(args)
    ZenhubRelease.new(id: args[:release_id],
                      title: args[:title],
                      description: args[:description])
  end

  def get_issues
    issues = call(URI("#{OptParser.options.zenhub_url}/p1/reports/release/#{id}/issues"))
    super(issues: issues)
  end
end

class GithubRelease < Release
  def self.create(args)
    puts "WARN You have #{args[:open_issues]} issues open" if args[:open_issues] > 0
    GithubRelease.new(id: args[:number],
                      title: args[:title],
                      description: args[:description])
  end

  def get_issues
    issues_with_prs = call(URI("#{OptParser.options.github_url}/issues?milestone=#{id}&state=all"))
    issues = issues_with_prs.select { |issue| issue[:pull_request].nil? }
    super(issues: issues)
  end
end

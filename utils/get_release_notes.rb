require 'uri'
require 'net/http'
require 'openssl'
require 'json'

PATH_TO_ZENHUB_TOKEN = 'token'
RN_OUT_PATH_PREFIX = ''

ZENHUB_URL = 'https://api.zenhub.io'
GITHUB_URL = 'https://api.github.com/repos/AbsaOss/enceladus'
REPO_ID = '154513089' # ENCELADUS REPO ID
ZENHUB_TOKEN = File.read(PATH_TO_ZENHUB_TOKEN).strip

class String
  def remove_first_line
    first_newline = (index("\n") || size - 1) + 1
    slice!(0, first_newline).sub("\n",'')
    self
  end
end

class Release
  attr_accessor :release_id, :title, :description, :start_date, :desired_end_date
  attr_accessor :created_at, :closed_at, :state, :issues

  def initialize(release_id:, title:, description:, start_date:, desired_end_date:, created_at:, closed_at:, state:)
    @release_id = release_id
    @title = title
    @description = description
    @start_date = start_date
    @desired_end_date = desired_end_date
    @created_at = created_at
    @closed_at = closed_at
    @state= state
    @date = Time.now
    @liquid = {
        layout: 'default',
        title: "Release v#{title}",
        tags: ["v#{title}", 'changelog'],
        excerpt_separator: '<!--more-->'
    }
    @liquid[:tags] << "hotfix" if title.split('.').last != '0'
  end

  def get_file_name
    "#{RN_OUT_PATH_PREFIX}#{@date.strftime('%Y-%m-%d')}-release-#{@title}.md"
  end

  def get_issues
    issues = call(URI("#{ZENHUB_URL}/p1/reports/release/#{release_id}/issues"))
    @issues = issues.inject([]) { |acc, v| acc << Issue.new(v[:issue_number]) }
  end

  def get_release_notes
    issue_list = ""
    issues.map do |issue|
      issue.get_release_notes
      issue_list << issue.make_release_note_string
    end

    release_notes = ""
    release_notes << "---\n"
    @liquid.each do |key, value|
      release_notes << "#{key}: #{value}\n"
    end
    release_notes << "---\n"
    release_notes << "As of #{@date.strftime('%d/%m %Y')} the new version #{@title} is out\n"
    release_notes << "<!--more-->\n\n"
    release_notes << issue_list
  end
end

class Issue
  attr_accessor :number, :release_comment

  def initialize(number)
    @number = number
  end

  def get_release_notes
    comments = call(URI("#{GITHUB_URL}/issues/#{@number}/comments"))
    release_notes = comments.select { |comment| comment[:body] =~ /^Release Notes.*/ }
    if release_notes.nil? || release_notes.empty?
      @release_comment = "Couldn't find Release Notes for #{number}"
    else
      @release_comment = release_notes.inject('') { |acc, note| "#{acc}#{note[:body].remove_first_line}\n" }
    end
  end

  def make_release_note_string
    "- [##{@number}]({{ site.github.issues_url }}/#{@number}) #{@release_comment}\n"
  end
end

def call(url)
  http = Net::HTTP.new(url.host, url.port)
  http.use_ssl = true
  http.verify_mode = OpenSSL::SSL::VERIFY_NONE

  request = Net::HTTP::Get.new(url)
  request["x-authentication-token"] = ZENHUB_TOKEN if url.host == 'api.zenhub.io'
  request["content-type"] = 'application/json'
  response = http.request(request)
  body_json = JSON.parse(response.body)
  keys_to_symbols(body_json)
end

def keys_to_symbols(whatever)
  if whatever.is_a?(Hash)
    whatever.inject({}) do |acc,(k,v)|
      acc[k.to_sym] = v
      acc
    end
  elsif whatever.is_a?(Array)
    whatever.inject([]) do |acc, v|
      acc << (v.is_a?(Hash) ? keys_to_symbols(v) : v)
      acc
    end
  else
    whatever
  end
end


all_releases = call(URI("#{ZENHUB_URL}/p1/repositories/#{REPO_ID}/reports/releases"))
latest_release = Release.new(all_releases.last)
latest_release.get_issues
release_notes = latest_release.get_release_notes
File.open(latest_release.get_file_name, 'w') { |file| file.write(release_notes) }

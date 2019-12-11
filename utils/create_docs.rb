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


require 'fileutils'

NEW_VERSION = ARGV[0]
PAGES_ROOT = File.expand_path('..', __dir__)
DOC_FOLDER = "#{PAGES_ROOT}/_docs"
VERSIONS_YAML = "#{PAGES_ROOT}/_data/versions.yaml"

unless NEW_VERSION =~ /[0-9]+\.[0-9]+\.[0-9]+/
  raise ArgumentError, "Version not in correct format", caller
end

FileUtils.cd(DOC_FOLDER, verbose: true) do
  last_version = Dir.glob('*').sort_by { |v| Gem::Version.new(v) }.last

  FileUtils.cp_r(last_version, NEW_VERSION, verbose: true)

  Dir.glob("#{NEW_VERSION}/*").each do |file_path|
    puts "Updating version for #{file_path}"
    file_content = File.read(file_path).partition( /---.*?(---)/m )
    new_content = file_content[0]
    new_content << file_content[1].gsub(last_version, NEW_VERSION)
    new_content << file_content[2]
    File.open(file_path, 'w') { |file| file.puts(new_content) }
  end
end

puts "Appending #{NEW_VERSION} to versions.yaml"
open(VERSIONS_YAML, 'a') { |f| f.puts "- '#{NEW_VERSION}'" }

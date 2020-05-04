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

module Docs
  def self.create_docs(doc_folder:, new_version:, latest_version:)
    FileUtils.cd(doc_folder, verbose: true) do
      FileUtils.cp_r(latest_version, new_version, verbose: true)

      remove_previous_redirect(version: latest_version)
      updated_versions(path: "#{new_version}/*", l_version: latest_version, n_version: new_version)
    end
  end

  def self.remove_previous_redirect(version:)
    Dir.glob("#{version}/*").each do |file_path|
      if file_path =~ /\.md$/
        puts "Removing redirects for #{file_path}"
        file_content = partition_liquid(file: File.read(file_path))
        new_content = file_content[0]
        new_content << remove_redirect(file_content[1])
        new_content << file_content[2]
        File.open(file_path, 'w') { |file| file.puts(new_content) }
      else
        remove_previous_redirect(version: "#{file_path}/*")
      end
    end
  end

  def self.updated_versions(path:, l_version:, n_version:)
    Dir.glob(path).each do |file_path|
      if file_path =~ /\.md$/
        puts "Updating version for #{file_path}"
        file_content = partition_liquid(file: File.read(file_path))
        new_content = file_content[0]
        new_content << file_content[1].gsub(l_version, n_version)
        new_content << file_content[2]
        File.open(file_path, 'w') { |file| file.puts(new_content) }
      else
        updated_versions(path: "#{file_path}/*", l_version: l_version, n_version: n_version)
      end
    end
  end

  def self.partition_liquid(file:)
    file.partition( /---.*?(---)/m )
  end

  def self.remove_redirect(content)
    front_matter = []
    content.split("\n").each do |line|
      front_matter << line unless line =~ /^redirect_from/
    end
    front_matter.join("\n")
  end

  def self.remove_docs(doc_folder:, version:)
    FileUtils.cd(doc_folder, verbose: true) do
      FileUtils.rm_r(version, verbose: true)
    end
  end

  def self.get_latest_doc_version(doc_folder:)
    FileUtils.cd(doc_folder, verbose: true) do
      Dir.glob('*').sort_by { |v| Gem::Version.new(v) }.last
    end
  end

  def self.append_version(new_version:, versions_path:)
    puts "Appending #{new_version} to versions.yaml"
    open(versions_path, 'a') { |f| f.puts "- '#{new_version}'" }
  end

  def self.reset_version_list(versions_path:, first_line: '')
    puts "Reseting/clearing versions.yaml"
    File.write(versions_path, first_line)
  end

  def self.add_topic(topic_name:, doc_folder:, yaml_path:)
    name_pretty = topic_name.tr('-', ' ').split.map(&:capitalize).join(' ')
    FileUtils.cd(doc_folder, verbose: true) do
      versions = Dir.glob('*')
      versions.each do |version|
        FileUtils.cd(version, verbose: true) do
          first_file = Dir.glob('*').first
          liquid = File.read(first_file).partition( /---.*?(---)/m )[1]
          new_liquid = liquid.gsub(/^title: \w*/, "title: #{name_pretty}")
          File.write("#{topic_name}.md", new_liquid)
        end
      end
    end

    data = YAML.load_file(yaml_path)
    data << { "name" => topic_name, "pretty_name" => name_pretty}

    File.open(yaml_path, 'w') {|f| f.write data.to_yaml }
  end

  def self.remove_topic(topic_name:, doc_folder:, yaml_path:)
    FileUtils.cd(doc_folder, verbose: true) do
      versions = Dir.glob('*')
      versions.each do |version|
        file_name = "#{version}/#{topic_name}.md"
        FireUtils.rm(file_name) if File.exist?(file_name)
      end
    end

    data = YAML.load_file(yaml_path).delete_if { |d| d["name"] == topic_name }
    File.open(yaml_path, 'w') {|f| f.write data.to_yaml }
  end
end

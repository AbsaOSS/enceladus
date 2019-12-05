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
    file_content = File.read(file_path)
    new_content = file_content.gsub(last_version, NEW_VERSION)
    File.open(file_path, 'w') { |file| file.puts(new_content) }
  end
end

puts "Appending #{NEW_VERSION} to versions.yaml"
open(VERSIONS_YAML, 'a') { |f| f.puts "- '#{NEW_VERSION}'" }

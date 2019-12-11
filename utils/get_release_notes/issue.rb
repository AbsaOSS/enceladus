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

class Issue
  attr_accessor :number, :release_comment, :empty

  def initialize(number:, title: nil)
    @number = number
    @title = title
    @empty = true
  end

  def get_release_notes
    comments = call(URI("#{OptParser.options.github_url}/issues/#{@number}/comments"))
    release_notes = comments.select do |comment|
      comment[:body] =~ /\A[[:blank:]]*(R|r)elease (N|n)otes([[:blank:]]|-|:)*\n/
    end

    if release_notes.nil? || release_notes.empty?
      @title = call(URI("#{OptParser.options.github_url}/issues/1102"))[:title] if @title.nil?
      @release_comment = "Couldn't find Release Notes for #{number} - #{@title}"
    else
      @empty = false
      @release_comment = release_notes.inject('') { |acc, note| "#{acc}#{note[:body].remove_first_line}" }
    end
  end

  def make_release_note_string
    "- [##{@number}]({{ site.github.issues_url }}/#{@number}) #{@release_comment}\n"
  end
end

require 'test_helper'
require 'ruby-prof'

class MongodbDriverTest < ActiveSupport::TestCase

  def setup
    @data = make_batch_data
    ItemMongoid.destroy_all
    ItemMongomapper.destroy_all
  end

  test "mongoid driver performance" do
    RubyProf.start
   # http://stackoverflow.com/questions/3772378/batch-insert-update-using-mongoid
    ItemMongoid.collection.insert(@data)
    res = RubyProf.stop

    puts " total data size: #{@data.size}"
    printer = RubyProf::FlatPrinter.new(res)
    printer.print

    items = ItemMongoid.all
    assert_equal @data.size, items.size
  end

  test "mongomapper driver performance" do
    RubyProf.start
    ItemMongomapper.create(@data)
    res = RubyProf.stop

    puts " total data size: #{@data.size}"
    printer = RubyProf::FlatPrinter.new(res)
    printer.print

    items = ItemMongomapper.all
    assert_equal @data.size, items.size
  end




  protected

    def make_batch_data
      raw = File.read("#{Rails.root}/doc/pipe.json")
      data = JSON.parse(raw)
      batch = []
      #(1..100).each do |i|
      (1..10).each do |i|
        count = 1
        data['value']['items'].each do |item|
          _key = "#{extract_guid(item).gsub(/\W/, '')}_#{i}_#{count}"
          batch << { :"#{_key}" => item }
          count += 1
        end
      end
      batch
    end

    def extract_guid(item)
      if item['guid'].is_a?(Hash) # facebook
        item['guid']['content']
      else
        item['guid']
      end
    end

end

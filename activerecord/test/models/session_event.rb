# frozen_string_literal: true

class SessionEvent < ActiveRecord::Base
  belongs_to :eventable, polymorphic: true
end

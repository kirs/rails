# frozen_string_literal: true

class ShoppingSession < ActiveRecord::Base
  self.primary_key = [:shop_id, :session_id]

  has_many :session_events, as: :eventable, primary_key: :legacy_id
end

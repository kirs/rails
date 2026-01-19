# frozen_string_literal: true

require "cases/helper"
require "models/shopping_session"
require "models/session_event"
require "models/toy"
require "models/sponsor"

class PolymorphicInversePrimaryKeyTest < ActiveRecord::TestCase
  def setup
    @session = ShoppingSession.create!(
      shop_id: "shop_1",
      session_id: "sess_abc",
      legacy_id: 12345,
      name: "Test Session"
    )
  end

  def teardown
    SessionEvent.delete_all
    ShoppingSession.delete_all
  end

  def test_setting_polymorphic_association_uses_inverse_primary_key
    # When the inverse association (has_many :session_events) specifies primary_key: :legacy_id,
    # setting the polymorphic belongs_to should use that column value for eventable_id
    event = SessionEvent.new(action: "click")
    event.eventable = @session

    assert_equal 12345, event.eventable_id
    assert_equal "ShoppingSession", event.eventable_type
  end

  def test_creating_with_polymorphic_association_uses_inverse_primary_key
    event = SessionEvent.create!(eventable: @session, action: "click")

    assert_equal 12345, event.eventable_id
    assert_equal "ShoppingSession", event.eventable_type
  end

  def test_loading_polymorphic_association_uses_inverse_primary_key
    event = SessionEvent.create!(eventable: @session, action: "click")

    # Reload to ensure we're fetching from DB
    event = SessionEvent.find(event.id)

    # The lookup should use legacy_id, not the composite primary key
    assert_equal @session, event.eventable
  end

  def test_inverse_association_query_uses_primary_key_option
    SessionEvent.create!(eventable: @session, action: "click")
    SessionEvent.create!(eventable: @session, action: "scroll")

    # Query through the has_many should use legacy_id
    events = @session.session_events.to_a

    assert_equal 2, events.size
    assert_equal ["click", "scroll"], events.map(&:action).sort
  end

  def test_polymorphic_with_standard_model_still_works
    # Create a standard model without composite PK to ensure we didn't break normal behavior
    toy = Toy.create!
    sponsor = Sponsor.create!(sponsorable: toy)

    assert_equal toy.toy_id, sponsor.sponsorable_id
    assert_equal toy, sponsor.reload.sponsorable
  end
end

class PolymorphicInversePrimaryKeyConsistencyTest < ActiveRecord::TestCase
  def test_uses_custom_primary_key_when_all_inverse_associations_agree
    # ShoppingSession already has: has_many :session_events, as: :eventable, primary_key: :legacy_id
    # Adding another association with the same primary_key should work
    ShoppingSession.has_many :audit_events_consistent, class_name: "SessionEvent", as: :eventable, primary_key: :legacy_id

    session = ShoppingSession.create!(shop_id: "shop_1", session_id: "sess_1", legacy_id: 111, name: "Test")
    event = SessionEvent.create!(eventable: session, action: "click")

    # Should use legacy_id since all associations agree
    assert_equal 111, event.eventable_id
    assert_equal session, event.reload.eventable
  ensure
    SessionEvent.delete_all
    ShoppingSession.delete_all
  end

  def test_allows_multiple_associations_with_different_primary_keys
    # This tests that we don't raise an error when associations have different primary_keys
    # The behavior in this case is to fall back to default PK lookup
    assert_nothing_raised do
      Class.new(ActiveRecord::Base) do
        def self.name; "TestSession"; end
        self.table_name = "shopping_sessions"
        self.primary_key = [:shop_id, :session_id]

        has_many :session_events, as: :eventable, primary_key: :legacy_id
        has_many :audit_events, class_name: "SessionEvent", as: :eventable, primary_key: :shop_id
      end
    end
  end

  def test_allows_different_polymorphic_names_with_different_primary_keys
    assert_nothing_raised do
      Class.new(ActiveRecord::Base) do
        def self.name; "MultiPolySession"; end
        self.table_name = "shopping_sessions"
        self.primary_key = [:shop_id, :session_id]

        has_many :session_events, as: :eventable, primary_key: :legacy_id
        has_many :sponsors, as: :sponsorable, primary_key: :shop_id  # Different polymorphic name, OK
      end
    end
  end
end

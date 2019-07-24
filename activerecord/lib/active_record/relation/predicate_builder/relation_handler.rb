# frozen_string_literal: true

module ActiveRecord
  class PredicateBuilder
    class RelationHandler # :nodoc:
      def call(attribute, value)
        if value.eager_loading?
          value = value.send(:apply_join_dependency)
        end

        if value.optimizer_hints_values
          value = value.unscope(:optimizer_hints)
        end

        if value.select_values.empty?
          value = value.select(value.arel_attribute(value.klass.primary_key))
        end

        attribute.in(value.arel)
      end
    end
  end
end

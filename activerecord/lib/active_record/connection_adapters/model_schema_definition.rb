# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    # ModelSchemaDefinition encapsulates schema information for a specific model
    # on a specific connection. It's owned and cached by the adapter, not the model.
    class ModelSchemaDefinition
      attr_reader :model_class, :connection

      def initialize(model_class, connection)
        @model_class = model_class
        @connection = connection
        @table_name = model_class.table_name

        # Cache expensive computations
        @primary_key = nil
        @composite_primary_key = nil
        @attributes_builder = nil
        @returning_columns_for_insert = nil
      end

      # Delegate to schema_cache for basic schema info
      def columns
        @columns ||= begin
          cols = @connection.schema_cache.columns(@table_name)
          # Filter based on model's only_columns or ignored_columns
          if @model_class.only_columns.present?
            cols.select { |col| @model_class.only_columns.include?(col.name.to_s) }
          elsif @model_class.ignored_columns.present?
            cols.reject { |col| @model_class.ignored_columns.include?(col.name.to_s) }
          else
            cols
          end
        end
      end

      def columns_hash
        @columns_hash ||= begin
          hash = @connection.schema_cache.columns_hash(@table_name)
          # Filter based on model's only_columns or ignored_columns
          if @model_class.only_columns.present?
            hash.slice(*@model_class.only_columns)
          elsif @model_class.ignored_columns.present?
            hash.except(*@model_class.ignored_columns)
          else
            hash
          end.freeze
        end
      end

      def column_names
        columns.map(&:name)
      end

      def primary_key
        @primary_key ||= begin
          pk = @connection.schema_cache.primary_keys(@table_name)
          # Single PK returns string, composite returns array
          pk = pk.first unless pk.is_a?(Array) && pk.size > 1
          pk
        end
      end

      def primary_key=(value)
        @primary_key = if value.is_a?(Array)
          value.map { |v| -v.to_s }.freeze
        elsif value
          -value.to_s
        end
        @composite_primary_key = value.is_a?(Array)
        # Clear cached attributes builder when primary key changes
        @attributes_builder = nil
        @returning_columns_for_insert = nil
      end

      def composite_primary_key?
        return @composite_primary_key unless @composite_primary_key.nil?
        @composite_primary_key = primary_key.is_a?(Array)
      end

      def attributes_builder
        @attributes_builder ||= begin
          # Get defaults from model's _default_attributes, excluding primary key columns
          pk_columns = Array(primary_key)

          # Build defaults hash
          defaults = {}
          columns_hash.each do |name, column|
            next if pk_columns.include?(name)
            if column.has_default?
              type = @model_class.type_for_column(@connection, column)
              defaults[name] = ActiveModel::Attribute.from_database(
                column.name,
                column.default,
                type
              )
            end
          end

          ActiveModel::AttributeSet::Builder.new(@model_class.attribute_types, defaults)
        end
      end

      def returning_columns_for_insert
        @returning_columns_for_insert ||= begin
          auto_populated = columns.filter_map do |column|
            column.name if @connection.return_value_after_insert?(column)
          end

          auto_populated.empty? ? Array(primary_key) : auto_populated
        end
      end

      # Clear cached values when schema changes
      def reload!
        @primary_key = nil
        @composite_primary_key = nil
        @attributes_builder = nil
        @returning_columns_for_insert = nil
        @columns = nil
        @columns_hash = nil
        self
      end
    end
  end
end
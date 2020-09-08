package za.co.absa.enceladus.utils.validation.field

import za.co.absa.enceladus.utils.schema.MetadataKeys
import za.co.absa.enceladus.utils.types.TypedStructField
import za.co.absa.enceladus.utils.validation.ValidationIssue

object DecimalFieldValidator extends NumericFieldValidator {
  override def validate(field: TypedStructField): Seq[ValidationIssue] = {
    super.validate(field) ++
      this.checkMetadataKey[Boolean](field, MetadataKeys.StrictParsing)
  }
}

package za.co.absa.enceladus.model.menas

case class AuditTrailChange(
  field: String,
  oldValue: Option[String],
  newValue: Option[String],
  message: String
)
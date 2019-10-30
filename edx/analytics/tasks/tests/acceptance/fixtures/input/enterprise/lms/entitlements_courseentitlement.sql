DROP TABLE IF EXISTS entitlements_courseentitlement;
CREATE TABLE entitlements_courseentitlement
(
  id                       INT AUTO_INCREMENT PRIMARY KEY,
  created                  DATETIME(6)  NOT NULL,
  modified                 DATETIME(6)  NOT NULL,
  uuid                     CHAR(32)     NOT NULL,
  course_uuid              CHAR(32)     NOT NULL,
  expired_at               DATETIME(6)  NULL,
  mode                     VARCHAR(100) NOT NULL,
  order_number             VARCHAR(128) NULL,
  enrollment_course_run_id INT          NULL,
  user_id                  INT          NOT NULL
);
INSERT INTO `entitlements_courseentitlement` VALUES
  (1,'2017-11-20 16:23:05.610538','2017-11-20 16:23:05.610851','fe25af9e3d394dd49ddbaa019bdfc2e8','38e5ea5533db4438a3846d90b4b31c33',NULL,'verified','EDX-157042',35,15);

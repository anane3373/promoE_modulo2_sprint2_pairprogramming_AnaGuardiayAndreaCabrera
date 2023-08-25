-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema energia
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema energia
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `energia` DEFAULT CHARACTER SET utf8 ;
USE `energia` ;

-- -----------------------------------------------------
-- Table `energia`.`fechas`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `energia`.`fechas` (
  `idfechas` INT NOT NULL AUTO_INCREMENT,
  `fecha` DATE NULL,
  PRIMARY KEY (`idfechas`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `energia`.`nacional_renovable_no_renovable`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `energia`.`nacional_renovable_no_renovable` (
  `idnacional_renovable_no_renovable` INT NOT NULL AUTO_INCREMENT,
  `porcentaje` INT NULL,
  `tipo_energia` VARCHAR(45) NULL,
  `valor` DECIMAL NULL,
  `fechas_idfechas` INT NOT NULL,
  PRIMARY KEY (`idnacional_renovable_no_renovable`),
  INDEX `fk_nacional_renovable_no_renovable_fechas_idx` (`fechas_idfechas` ASC) VISIBLE,
  CONSTRAINT `fk_nacional_renovable_no_renovable_fechas`
    FOREIGN KEY (`fechas_idfechas`)
    REFERENCES `energia`.`fechas` (`idfechas`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `energia`.`comunidades`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `energia`.`comunidades` (
  `idcomunidades` INT NOT NULL AUTO_INCREMENT,
  `comunidades` VARCHAR(45) NULL,
  PRIMARY KEY (`idcomunidades`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `energia`.`comunidades_renovable_no_renovable`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `energia`.`comunidades_renovable_no_renovable` (
  `idcomunidades_renovable_no_renovable` INT NOT NULL AUTO_INCREMENT,
  `porcentaje` INT NULL,
  `tipo_energia` VARCHAR(45) NULL,
  `valor` DECIMAL NULL,
  `fechas_idfechas` INT NOT NULL,
  `comunidades_idcomunidades` INT NOT NULL,
  PRIMARY KEY (`idcomunidades_renovable_no_renovable`),
  INDEX `fk_comunidades_renovable_no_renovable_fechas1_idx` (`fechas_idfechas` ASC) VISIBLE,
  INDEX `fk_comunidades_renovable_no_renovable_comunidades1_idx` (`comunidades_idcomunidades` ASC) VISIBLE,
  CONSTRAINT `fk_comunidades_renovable_no_renovable_fechas1`
    FOREIGN KEY (`fechas_idfechas`)
    REFERENCES `energia`.`fechas` (`idfechas`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_comunidades_renovable_no_renovable_comunidades1`
    FOREIGN KEY (`comunidades_idcomunidades`)
    REFERENCES `energia`.`comunidades` (`idcomunidades`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;

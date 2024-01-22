package org.cosmo;

import com.codeborne.selenide.Condition;
import com.codeborne.selenide.Configuration;
import com.codeborne.selenide.Selenide;
import com.codeborne.selenide.SelenideElement;
import org.openqa.selenium.By;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;

import static com.codeborne.selenide.Selectors.*;
import static com.codeborne.selenide.Selenide.*;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            throw new IllegalArgumentException("Usage: <remote url> <operation> <username> <password> <target file>");
        }
        String remoteUrl = args[0];
        DocType docType = DocType.fromName(args[1]);
        if (docType == DocType.STOCKS && args.length != 5) {
            throw new IllegalArgumentException("Usage: <remote url> <operation> <username> <password> <target file>");
        }
        String userName = args[2];
        String password = args[3];
        String targetFile = args[4];

        Configuration.headless = false;
        Configuration.remote = remoteUrl;//"http://localhost:4444/wd/hub";
        Configuration.browser = "chrome";
        LOGGER.info("Open main page");
        open("https://business.kazanexpress.ru/seller/signin");
        LOGGER.info("Login");
        $(By.id("username")).shouldBe(Condition.visible).setValue(userName);
        $(By.id("password")).shouldBe(Condition.visible).setValue(password);
        LOGGER.info("Click login button");
        $("[data-test-id='button__next']").click();
        LOGGER.info("Click reports button");
        $(byText("Отчеты")).shouldBe(Condition.visible, Duration.ofMillis(10000)).click();

        LOGGER.info("Click create report button");
        $("[data-test-id='button__report-creation']").shouldBe(Condition.visible, Duration.ofMillis(10000)).click();
        LOGGER.info("Click report type button");
        $(byText("Тип отчета")).shouldBe(Condition.visible, Duration.ofMillis(10000)).click();
        LOGGER.info("Click stock button");
        $(byText("Остатки")).shouldBe(Condition.visible, Duration.ofMillis(10000)).click();
        LOGGER.info("Click market button");
        $(byText("Магазин")).shouldBe(Condition.visible, Duration.ofMillis(10000)).click();
        LOGGER.info("Click select all button");
        $(byText("Выбрать все")).shouldBe(Condition.visible, Duration.ofMillis(10000)).click();
        LOGGER.info("Click create report button");
        $(byText("Отчет")).click();
        LOGGER.info("Click csv button");
        $(byText("CSV")).shouldBe(Condition.visible, Duration.ofMillis(10000)).click();
        LOGGER.info("Click create report button");
        $(byText("Сформировать")).shouldBe(Condition.enabled, Duration.ofMillis(10000)).shouldBe(Condition.visible).click();
        LOGGER.info("Wait for text: Ограничение создания отчета в 1 минуту");
        $(byText("Ограничение создания отчета в 1 минуту")).shouldBe(Condition.visible, Duration.ofMillis(10000));
        LOGGER.info("Click close modal button");
        $("[data-test-id='button__close-modal']").shouldBe(Condition.visible, Duration.ofSeconds(30)).click();
        LOGGER.info("Looking for today report");
        Selenide.$(withTextCaseInsensitive("Формируется отчет")).shouldBe(Condition.visible, Duration.ofSeconds(30));

        LOGGER.info("Waiting for today report is visible");
        $(withTextCaseInsensitive("Формируется отчет")).shouldNotBe(Condition.exist, Duration.ofSeconds(200));
        // Получаем родительский элемент
        // Находим элемент с классом "download-icon" внутри reportCardElement
        LOGGER.info("Waiting download icon and click");
        SelenideElement downloadIconElement = $$(by("data-test-id", "report-card__download"))
                .first()
                .shouldBe(Condition.visible, Duration.ofSeconds(200));

        //*[@id="main-section"]/div/div[2]/div[2]/div[3]/a


        LOGGER.info("Download file");
        File file = downloadIconElement.download();
        LOGGER.info("Downloaded file: {}", file.getAbsolutePath());
        Path sourcePath = Path.of(file.getAbsolutePath());
        Path targetDirectoryPath = Path.of(targetFile);
        LOGGER.info("Copy file from {} to {}", sourcePath, targetDirectoryPath);
        Files.copy(sourcePath, targetDirectoryPath, StandardCopyOption.REPLACE_EXISTING);
        LOGGER.info("Finish");
    }
}
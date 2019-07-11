package com.tesco.aqueduct.pipe.http;

import com.tesco.aqueduct.pipe.api.Message;
import com.tesco.aqueduct.pipe.api.MessageWriter;
import io.micronaut.http.annotation.*;

import javax.inject.Inject;
import java.util.List;

@Controller
public class PipeWriteController {

    // Using property injection until micronaut test framework is stable
    @Inject
    private MessageWriter messageWriter;

    @Post("/pipe")
    public Status writeMessages(@Body final List<Message> messages) {
        messageWriter.write(messages);
        return Status.ok();
    }
}

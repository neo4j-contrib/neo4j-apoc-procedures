$(document).ready(function() {
    $("footer").prepend(`
            <div align="center">
            
                <div id="feedback-form">
                    <h4>Was the information on this page helpful?</h4>
                    <div style="font-size: 3em;">
                        <span><img data-helpful="yes" class="feedback" style="padding-bottom:0; cursor:pointer;" src="https://s3.amazonaws.com/dev.assets.neo4j.com/wp-content/uploads/2020/feedback_happy.png" width="100px" /></span>
                        <span><img data-helpful="no" class="feedback"  style="padding-bottom:0;cursor:pointer;" src="https://s3.amazonaws.com/dev.assets.neo4j.com/wp-content/uploads/2020/feedback_sad.png" width="100px" /></span>
                    </div>
            
                </div>
            </div>`)

    $("img.feedback").click(function (event) {
        const documentHelpful = event.target.attributes["data-helpful"].value

        if ("yes" === documentHelpful) {
            $.post("https://uglfznxroe.execute-api.us-east-1.amazonaws.com/dev/Feedback", { helpful: true, url: window.location.href });
            $("div#feedback-form").html(`<h4>Thanks for your feedback. We're happy to hear that the information on this page was helpful <img style="padding-bottom:0;" src="https://s3.amazonaws.com/dev.assets.neo4j.com/wp-content/uploads/2020/feedback_happy.png" width="30px" />  </h4>`)
        } else {
            const specificFeedback = `
                    <div id="specific-feedback" style="display: inline-block; text-align: left;">
                        <h4>Thanks for your feedback. How can we improve this page?</h4>
                        <div>
                            <input type="radio" data-reason="missing" name="specific" value="missing" checked="true" />
                            <label for="missing">It has missing information</label>
                        </div>
                
                        <div>
                            <input type="radio" data-reason="hard-to-follow" name="specific" value="hard-to-follow" />
                            <label for="hard-to-follow">It's hard to follow or confusing</label>
                        </div>
                        
                        <div>
                            <input type="radio" data-reason="inaccurate" name="specific" value="inaccurate" />
                            <label for="hard-to-follow">It's inaccurate, out of date, or doesn't work</label>
                        </div>
                        
                        
                        <div>
                            <input type="radio" data-reason="other" name="specific" value="other" />
                            <label for="hard-to-follow">It has another problem not covered by the above</label>
                        </div>                        
                
                
                        <div style="padding-top:5px">                
                            <p><label for="more-information">More information</label>  </p>
                            <textarea type="text" rows="3" cols="50" name="more-information" style="resize:none"></textarea>
                        </div>
                
                        <div style="padding:10px 0 10px 0;" class="submit-extra-feedback">
                
                            <input type="button" data-submit="submit" value="Submit feedback" />
                            <input type="button" data-submit="skip" value="Skip" />
                        </div>                                
                    </div>
                `
            $("div#feedback-form").html(specificFeedback)
        }
    });

    $(document).on("click", "div.submit-extra-feedback input", function(event) {
        const submitType = event.target.attributes["data-submit"].value

        if("skip" === submitType) {
            $.post("https://uglfznxroe.execute-api.us-east-1.amazonaws.com/dev/Feedback", { helpful: false, url: window.location.href });
            $("div#feedback-form").html("<h4>Thanks for your feedback. We'll take it account when we're updating our documentation</h4>")
        } else {

            const reason = $("input[name='specific']:checked")[0].attributes["data-reason"].value
            const moreInformation = $("textarea[name='more-information']")[0].value

            $.post("https://uglfznxroe.execute-api.us-east-1.amazonaws.com/dev/Feedback", { helpful: false, url: window.location.href, reason: reason, moreInformation: moreInformation });
            $("div#feedback-form").html("<h4>Thanks for your feedback. We'll take it account when we're updating our documentation</h4>")
        }
    });
} );